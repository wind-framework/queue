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
    private $btimeout = 10;

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

        if ($concurrent < $this->btimeout) {
            $this->btimeout = $concurrent;
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

            list(, $index) = yield $this->redis->blPop($this->keysReady, $this->btimeout);
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
                yield $this->removeIndex($message);
                return true;
            } else {
                return false;
            }
        });

    }

    public function fail(Message $message)
    {
        return call(function() use ($message) {
            if (yield $this->removeIndex($message)) {
                return yield $this->redis->rPush($this->keyFail, $message->raw);
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

    private function removeIndex(Message $message)
    {
        return call(function() use ($message) {
            return (yield $this->redis->zRem($this->keyReserved, $message->raw)) > 0;
        });
    }

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
