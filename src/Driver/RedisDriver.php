<?php

namespace Wind\Queue\Driver;

use Wind\Queue\Job;
use Wind\Queue\Message;
use Wind\Queue\Queue;
use Wind\Redis\Redis;
use Wind\Utils\StrUtil;
use function Amp\call;

class RedisDriver implements Driver
{

    /**
     * @var Redis
     */
    private $redis;
    private $blockTimeout = 10;

    private $keysReady = [];
    private $keyReserved;
    private $keyDelay;
    private $keyFail;
    private $keyData;
    private $keyId;

    /**
     * @param array $config
     */
    public function __construct($config)
    {
        $this->redis = empty($config['use_single_instance']) ? di()->make(Redis::class) : di()->get(Redis::class);

        $rk = $config['key'].':ready';
        $this->keysReady = [
            Queue::PRI_HIGH  => $rk.':high',
            Queue::PRI_NORMAL  => $rk.':normal',
            Queue::PRI_LOW  => $rk.':low'
        ];
        $this->keyReserved = $config['key'].':reserved';
        $this->keyDelay = $config['key'].':delay';
        $this->keyFail = $config['key'].':fail';
        $this->keyData = $config['key'].':data';
        $this->keyId = $config['key'].':id';

        //轮询需要有间隔主要作用于延迟队列的转移，在有多个并发时每个并发都有可能进行转移处理，理想情况下每秒都有协程处理到轮询。
        //所以并发多时，适当的增加轮询可间隔可以减少性能浪费
        $processes = $config['processes'] ?? 1;
        $concurrent = $config['concurrent'] ?? 1;
        $concurrent *= $processes;

        if ($concurrent < $this->blockTimeout) {
            $this->blockTimeout = $concurrent;
        }

        $this->uniq = StrUtil::randomString(8);
    }

    public function connect()
    {
        return $this->redis->connect();
    }

    public function push(Message $message, $delay)
    {
        return call(function() use ($message, $delay) {
            $message->id = yield $this->redis->incr($this->keyId);

            //put data
            $data = \serialize($message->job);
            yield $this->redis->hSet($this->keyData, $message->id, $data);

            //put index
            $index = self::serializeIndex($message);

            if ($delay == 0) {
                $queue = $this->getPriorityKey($message->priority);
                yield $this->redis->rPush($queue, $index);
            } else {
                yield $this->redis->zAdd($this->keyDelay, time()+$delay, $index);
            }

            return $message->id;
        });
    }

    public function pop()
    {
        return call(function() {
            yield $this->ready($this->keyDelay);
            yield $this->ready($this->keyReserved);

            list(, $index) = yield $this->redis->blPop($this->keysReady, $this->blockTimeout);
            if ($index === null) {
                return null;
            }

            //get data
            list($id, $priority, $attempts) = self::unserializeIndex($index);
            $data = yield $this->redis->hGet($this->keyData, $id);

            //message is already deleted
            if (!$data) {
                return null;
            }

            /* @var Job $job */
            $job = \unserialize($data);
            yield $this->redis->zAdd($this->keyReserved, time()+$job->ttr, $index);

            $message = new Message($job, $id, $index);
            $message->priority = $priority;
            $message->attempts = $attempts;

            return $message;
        });
    }

    public function ack(Message $message)
    {
        return call(function() use ($message) {
            if (yield $this->redis->hDel($this->keyData, $message->id)) {
                return yield $this->removeIndex($message);
            } else {
                return false;
            }
        });

    }

    public function fail(Message $message)
    {
        return call(function() use ($message) {
            if (yield $this->redis->rPush($this->keyFail, $message->raw)) {
                return yield $this->removeIndex($message);
            } else {
                return false;
            }
        });
    }

    public function release(Message $message, $delay)
    {
        return call(function() use ($message, $delay) {
            if (yield $this->removeIndex($message)) {
                $message->attempts++;
                $index = self::serializeIndex($message);
                return $this->redis->zAdd($this->keyDelay, time() + $delay, $index);
            }
            return false;
        });
    }

    public function delete($id)
    {
        return $this->redis->hDel($this->keyData, $id);
    }

    public function attempts(Message $message) {
        return $message->attempts;
    }

    /**
     * Remove index stored in reserved list.
     *
     * @param Message $message
     * @return Promise<bool>
     */
    private function removeIndex(Message $message)
    {
        return call(function() use ($message) {
            return (yield $this->redis->zRem($this->keyReserved, $message->raw)) > 0;
        });
    }

    /**
     * Make delayed queue to ready list
     *
     * @param string $queue
     */
    private function ready($queue)
    {
        return call(function() use ($queue) {
            $now = time();
            $options = ['LIMIT', 0, 128];
            if ($expires = yield $this->redis->zrevrangebyscore($queue, $now, '-inf', $options)) {
                foreach ($expires as $index) {
                    if ((yield $this->redis->zRem($queue, $index)) > 0) {
                        list(, $priority) = self::unserializeIndex($index);
                        $key = $this->getPriorityKey($priority);
                        yield $this->redis->rPush($key, $index);
                    }
                }
            }
        });
    }

    /**
     * @inheritDoc
     */
    public function peekFail() {
        return call(function() {
            $index = yield $this->redis->lIndex($this->keyFail, 0);

            if (!$index) {
                return null;
            }

            $message = yield $this->peekByIndex($index);

            //Remove key if data not exists
            if ($message === null) {
                yield $this->redis->lRem($this->keyFail, $index, 1);
                return null;
            }

            return $message;
        });
    }

    /**
     * @inheritDoc
     */
    public function peekDelayed() {
        return call(function() {
            $result = yield $this->redis->zRange($this->keyDelay, 0, 0, 'WITHSCORES');

            if (!$result) {
                return null;
            }

            list($index, $timestamp) = $result;

            $message = yield $this->peekByIndex($index);
            $message->delayed = (int)$timestamp;

            //Remove key if data not exists
            if ($message === null) {
                yield $this->redis->zRem($this->keyDelay, $index);
            }

            return $message;
        });
    }

    public function peekReady() {
        //Todo: To be implement peekReady
    }

    private function peekByIndex($index)
    {
        return call(function() use ($index) {
            //get data
            list($id, $priority, $attempts) = self::unserializeIndex($index);
            $data = yield $this->redis->hGet($this->keyData, $id);

            //message is already deleted
            if (!$data) {
                return null;
            }

            /* @var Job $job */
            $job = \unserialize($data);
            $message = new Message($job, $id, $index);
            $message->priority = $priority;
            $message->attempts = $attempts;

            return $message;
        });
    }

    public function wakeupJob($id) {
        //Todo: To be implement wakeupJob
    }

    public function wakeup($num) {
        //Todo: To be implement wakeup
    }

    /**
     * @inheritDoc
     */
    public function drop($num) {
        return call(function() use ($num) {
            for ($i=0; $i<$num; $i++) {
                $index = yield $this->redis->lPop($this->keyFail);
                if ($index === null) {
                    return $i;
                }

                list($id) = self::unserializeIndex($index);
                yield $this->redis->hDel($this->keyData, $id);
            }

            return $num;
        });
    }

    public function stats() {
        return call(function() {
            $data = [
                'fails' => yield $this->redis->lLen($this->keyFail),
                'ready' => 0,
                'delayed' => yield $this->redis->zCard($this->keyDelay),
                'reserved' => yield $this->redis->zCard($this->keyReserved),
                'total_jobs' => 0,
                'next_id' => yield $this->redis->get($this->keyId)
            ];

            foreach ($this->keysReady as $key) {
                $data['ready'] += yield $this->redis->lLen($key);
            }

            $data['total_jobs'] = $data['next_id'];

            $info = yield $this->redis->info('server');
            preg_match_all('/(\w+):([^\r\n]+)/i', $info, $matchs);
            $infoArr = array_combine($matchs[1], $matchs[2]);
            $data['server'] = "Redis {$infoArr['redis_version']} ({$infoArr['os']})";

            return $data;
        });
    }

    private function getPriorityKey($pri)
    {
        return $this->keysReady[$pri] ?? $this->keysReady[Queue::PRI_NORMAL];
    }

    private static function serializeIndex(Message $message)
    {
        return $message->id.','.$message->priority.','.$message->attempts;
    }

    /**
     * @param string $index
     * @return array [id, priority, attempts]
     */
    private static function unserializeIndex($index)
    {
        return explode(',', $index);
    }

    public static function isSupportReuseConnection()
    {
        return true;
    }

}
