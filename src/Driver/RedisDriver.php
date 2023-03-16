<?php

namespace Wind\Queue\Driver;

use Wind\Queue\Job;
use Wind\Queue\Message;
use Wind\Queue\Queue;
use Wind\Redis\Redis;
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
        //only default connection and use_single_instance will persist
        $connection = isset($config['connection']) ? $config['connection'] : 'default';
        $persist = $connection == 'default' && !empty($config['use_single_instance']);
        $this->redis = $persist ? di()->get(Redis::class) : di()->make(Redis::class, compact('connection'));

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

        if (isset($config['block_timeout'])) {
            $this->blockTimeout = $config['block_timeout'];
        } else {
            //轮询需要有间隔主要作用于延迟队列的转移，在有多个并发时每个并发都有可能进行转移处理，理想情况下每秒都有协程处理到轮询。
            //而又由于 Redis 队列支持连接复用，所以实际上一个队列在一个进程中只有一个 Popper，所以 Redis 的连接超时时间其实是跟进程数有关。
            $processes = $config['processes'] ?? 1;

            if ($processes < $this->blockTimeout) {
                $this->blockTimeout = $processes;
            }
        }
    }

    public function connect()
    {
        return $this->redis->connect();
    }

    public function push(Message $message, $delay)
    {
        return call(function() use ($message, $delay) {
            $message->id = yield $this->redis->incr($this->keyId);

            yield $this->redis->transaction(function($transaction) use ($message, $delay) {
                /**
                 * @var \Wind\Redis\Transaction $transaction
                 */
                $data = \serialize($message->job);
                $index = self::serializeIndex($message);

                //put data
                yield $transaction->hSet($this->keyData, $message->id, $data);

                //put index
                if ($delay == 0) {
                    $queue = $this->getPriorityKey($message->priority);
                    yield $transaction->rPush($queue, $index);
                } else {
                    yield $transaction->zAdd($this->keyDelay, time()+$delay, $index);
                }
            });

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

            //Todo: 如果在 blpop 和 zadd 之间发生中断，那么索引就从 ready 状态中丢失，数据在 data 中变成幽灵状态

            /** @var Job $job */
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
        return $this->redis->transaction(function($transaction) use ($message, $delay) {
            /** @var \Wind\Redis\Transaction $transaction */

            $index = $message->raw;
            yield $transaction->zRem($this->keyReserved, $index);

            $message->attempts++;
            $index = self::serializeIndex($message);
            yield $this->redis->zAdd($this->keyDelay, time() + $delay, $index);

            return true;
        });
    }

    public function delete($id)
    {
        return $this->redis->hDel($this->keyData, $id);
    }

    public function touch(Message $message)
    {
        return $this->redis->transaction(function($transaction) use ($message) {
            /** @var \Wind\Redis\Transaction $transaction */

            $index = $message->raw;
            yield $transaction->zRem($this->keyReserved, $index);
            yield $transaction->zAdd($this->keyReserved, time()+$message->job->ttr, $index);
        });
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
            $options = ['LIMIT', 0, 256];
            if ($expires = yield $this->redis->zrevrangebyscore($queue, $now, '-inf', $options)) {
                foreach ($expires as $index) {
                    //Todo: fix
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

            //Remove key if data not exists
            if ($message === null) {
                yield $this->redis->zRem($this->keyDelay, $index);
            } else {
                $delayed = $timestamp - time();
                $message->delayed = $delayed > 0 ? $delayed : 0;
            }

            return $message;
        });
    }

    public function peekReady() {
        return call(function() {
            $index = (yield $this->redis->lIndex($this->keysReady[Queue::PRI_HIGH], 0))
                ?: (yield $this->redis->lIndex($this->keysReady[Queue::PRI_NORMAL], 0))
                ?: (yield $this->redis->lIndex($this->keysReady[Queue::PRI_LOW], 0));

            if (!$index) {
                return null;
            }

            return yield $this->peekByIndex($index);
        });
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

    public function wakeup($num) {
        return call(function() use ($num) {
            for ($i=0; $i<$num; $i++) {
                $index = yield $this->redis->lPop($this->keyFail);
                if ($index === null) {
                    return $i;
                }

                list(, $priority) = self::unserializeIndex($index);
                $queue = $this->getPriorityKey($priority);
                yield $this->redis->rPush($queue, $index);
            }

            return $num;
        });
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
                'total_jobs' => yield $this->redis->get($this->keyId)
            ];

            foreach ($this->keysReady as $key) {
                $data['ready'] += yield $this->redis->lLen($key);
            }

            $info = yield $this->redis->info('server');
            preg_match_all('/(\w+):([^\r\n]+)/i', $info, $matchs);
            $infoArr = array_combine($matchs[1], $matchs[2]);
            $data['server'] = "Redis {$infoArr['redis_version']} ({$infoArr['os']})";
            $data['uptime'] = $infoArr['uptime_in_seconds'];

            return $data;
        });
    }

    private function getPriorityKey($pri)
    {
        return $this->keysReady[$pri] ?? $this->keysReady[Queue::PRI_NORMAL];
    }

    /**
     * Serialize message index for redis
     *
     * You need re-serialize when any of id, priority or attempts property changed.
     *
     * @param Message $message
     * @return string
     */
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
