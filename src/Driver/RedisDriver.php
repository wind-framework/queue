<?php

namespace Wind\Queue\Driver;

use Wind\Queue\Job;
use Wind\Queue\Message;
use Wind\Queue\Queue;
use Wind\Redis\Redis;
use Wind\Utils\StrUtil;

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
        $this->redis = $persist ? di()->get(Redis::class) : new Redis($connection);

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

    // public function connect()
    // {
    //     return $this->redis->connect();
    // }

    public function push(Message $message, $delay)
    {
        $message->id = $this->redis->incr($this->keyId);

        $this->redis->transaction(function($transaction) use ($message, $delay) {
            /**
             * @var \Wind\Redis\Transaction $transaction
             */
            $data = \serialize($message->job);
            $index = self::serializeIndex($message);

            //put data
            $transaction->hSet($this->keyData, $message->id, $data);

            //put index
            if ($delay == 0) {
                $queue = $this->getPriorityKey($message->priority);
                $transaction->rPush($queue, $index);
            } else {
                $transaction->zAdd($this->keyDelay, time()+$delay, $index);
            }
        });

        return $message->id;
    }

    public function pop()
    {
        $this->ready($this->keyDelay);
        $this->ready($this->keyReserved);

        list(, $index) = $this->redis->blPop($this->keysReady, $this->blockTimeout);
        if ($index === null) {
            return null;
        }

        //get data
        list($id, $priority, $attempts) = self::unserializeIndex($index);
        $data = $this->redis->hGet($this->keyData, $id);

        //message is already deleted
        if (!$data) {
            return null;
        }

        /** @var Job $job */
        $job = \unserialize($data);
        $this->redis->zAdd($this->keyReserved, time()+$job->ttr, $index);

        $message = new Message($job, $id, $index);
        $message->priority = $priority;
        $message->attempts = $attempts;

        return $message;
    }

    public function ack(Message $message)
    {
        if ($this->redis->hDel($this->keyData, $message->id)) {
            return $this->removeIndex($message);
        } else {
            return false;
        }
    }

    public function fail(Message $message)
    {
        if ($this->redis->rPush($this->keyFail, $message->raw)) {
            return $this->removeIndex($message);
        } else {
            return false;
        }
    }

    public function release(Message $message, $delay)
    {
        if ($this->removeIndex($message)) {
            ++$message->attempts;
            $index = self::serializeIndex($message);
            return $this->redis->zAdd($this->keyDelay, time() + $delay, $index);
        }
        return false;
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
     * @return bool
     */
    private function removeIndex(Message $message)
    {
        return $this->redis->zRem($this->keyReserved, $message->raw) > 0;
    }

    /**
     * Make delayed queue to ready list
     *
     * @param string $queue
     */
    private function ready($queue)
    {
        $now = time();
        $options = ['LIMIT', 0, 256];
        if ($expires = $this->redis->zrevrangebyscore($queue, $now, '-inf', $options)) {
            foreach ($expires as $index) {
                if (($this->redis->zRem($queue, $index)) > 0) {
                    list(, $priority) = self::unserializeIndex($index);
                    $key = $this->getPriorityKey($priority);
                    $this->redis->rPush($key, $index);
                }
            }
        }
    }

    /**
     * @inheritDoc
     */
    public function peekFail() {
        $index = $this->redis->lIndex($this->keyFail, 0);

        if (!$index) {
            return null;
        }

        $message = $this->peekByIndex($index);

        //Remove key if data not exists
        if ($message === null) {
            $this->redis->lRem($this->keyFail, $index, 1);
            return null;
        }

        return $message;
    }

    /**
     * @inheritDoc
     */
    public function peekDelayed() {
        $result = $this->redis->zRange($this->keyDelay, 0, 0, 'WITHSCORES');

        if (!$result) {
            return null;
        }

        list($index, $timestamp) = $result;

        $message = $this->peekByIndex($index);

        //Remove key if data not exists
        if ($message === null) {
            $this->redis->zRem($this->keyDelay, $index);
        } else {
            $delayed = $timestamp - time();
            $message->delayed = $delayed > 0 ? $delayed : 0;
        }

        return $message;
    }

    public function peekReady() {
        $index = $this->redis->lIndex($this->keysReady[Queue::PRI_HIGH], 0)
            ?: $this->redis->lIndex($this->keysReady[Queue::PRI_NORMAL], 0)
            ?: $this->redis->lIndex($this->keysReady[Queue::PRI_LOW], 0);

        if (!$index) {
            return null;
        }

        return $this->peekByIndex($index);
    }

    private function peekByIndex($index)
    {
        //get data
        list($id, $priority, $attempts) = self::unserializeIndex($index);
        $data = $this->redis->hGet($this->keyData, $id);

        //message is already deleted
        if (!$data) {
            return null;
        }

        /** @var Job $job */
        $job = \unserialize($data);
        $message = new Message($job, $id, $index);
        $message->priority = $priority;
        $message->attempts = $attempts;

        return $message;
    }

    public function wakeup($num) {
        for ($i=0; $i<$num; $i++) {
            $index = $this->redis->lPop($this->keyFail);
            if ($index === null) {
                return $i;
            }

            list(, $priority) = self::unserializeIndex($index);
            $queue = $this->getPriorityKey($priority);
            $this->redis->rPush($queue, $index);
        }

        return $num;
    }

    /**
     * @inheritDoc
     */
    public function drop($num) {
        for ($i=0; $i<$num; $i++) {
            $index = $this->redis->lPop($this->keyFail);
            if ($index === null) {
                return $i;
            }

            list($id) = self::unserializeIndex($index);
            $this->redis->hDel($this->keyData, $id);
        }

        return $num;
    }

    public function stats() {
        $data = [
            'fails' => $this->redis->lLen($this->keyFail),
            'ready' => 0,
            'delayed' => $this->redis->zCard($this->keyDelay),
            'reserved' => $this->redis->zCard($this->keyReserved),
            'total_jobs' => 0,
            'next_id' => $this->redis->get($this->keyId)
        ];

        foreach ($this->keysReady as $key) {
            $data['ready'] += $this->redis->lLen($key);
        }

        $data['total_jobs'] = $data['next_id'];

        $info = $this->redis->info('server');
        preg_match_all('/(\w+):([^\r\n]+)/i', $info, $matchs);
        $infoArr = array_combine($matchs[1], $matchs[2]);
        $data['server'] = "Redis {$infoArr['redis_version']} ({$infoArr['os']})";

        return $data;
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
