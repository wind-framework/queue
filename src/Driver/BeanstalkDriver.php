<?php

namespace Wind\Queue\Driver;

use Wind\Beanstalk\Beanstalk;
use Wind\Beanstalk\BeanstalkException;
use Wind\Queue\Message;
use Wind\Queue\Queue;

class BeanstalkDriver implements Driver
{

    /**
     * @var Beanstalk
     */
    private $client;

    /**
     * Tube to use
     *
     * @var string
     */
    private $tube;

    /**
     * Reserve timeout seconds, or null to no timeout
     *
     * @var int|null
     */
    private $reserveTimeout = null;

    public function __construct($config)
    {
        $this->client = new Beanstalk($config['host'], $config['port'], [
            'autoReconnect' => true,
            'concurrent' => true
        ]);

        //$this->client->debug = true;
        $this->tube = $config['tube'];

        if (isset($config['reserve_timeout']) && $config['reserve_timeout'] > 0) {
            $this->reserveTimeout = $config['reserve_timeout'];
        }

        $this->connect();
    }

    private function connect()
    {
        $this->client->connect();

        if ($this->tube != 'default') {
            $this->client->watch($this->tube);
            $this->client->ignore('default');
            $this->client->useTube($this->tube);
        }
    }

    public function push(Message $message, $delay)
    {
        $raw = serialize($message->job);

        switch ($message->priority) {
            case Queue::PRI_NORMAL: $pri = Beanstalk::DEFAULT_PRI; break;
            case Queue::PRI_HIGH: $pri = 512; break;
            case Queue::PRI_LOW: $pri = 2048; break;
            default: $pri = $message->priority;
        }

        return $this->client->put($raw, $pri, $delay, $message->job->ttr);
    }

    public function pop()
    {
        try {
            $data = $this->client->reserve($this->reserveTimeout);
            $job = unserialize($data['body']);
            return new Message($job, $data['id']);
        } catch (BeanstalkException $e) {
            switch ($e->getMessage()) {
                case 'DEADLINE_SOON':
                case 'TIMED_OUT': return null;
                default: throw $e;
            }
        }
    }

    public function ack(Message $message)
    {
        return $this->client->delete($message->id);
    }

    public function fail(Message $message)
    {
        return $this->client->bury($message->id);
    }

    public function release(Message $message, $delay)
    {
        $state = $this->client->statsJob($message->id);
        return $this->client->release($message->id, $state['pri'], $delay);
    }

    public function attempts(Message $message)
    {
        $state = $this->client->statsJob($message->id);
        return $state['releases'];
    }

    public function delete($id)
    {
        return $this->client->delete($id);
    }

    public function touch(Message $message) {
        return $this->client->touch($message->id);
    }

    public function peekDelayed() {
        return $this->peekWith('peekDelayed');
    }

    public function peekReady() {
        return $this->peekWith('peekReady');
    }

    public function peekFail()
    {
        return $this->peekWith('peekBuried');
    }

    public function wakeup($num) {
        return $this->client->kick($num);
    }

    /**
     * @inheritDoc
     */
    public function drop($num) {
        for ($i=0; $i<$num; $i++) {
            try {
                $data = $this->client->peekBuried();
                $this->client->delete($data['id']);
            } catch (BeanstalkException $e) {
                if ($e->getMessage() == 'NOT_FOUND') {
                    return $i;
                } else {
                    throw $e;
                }
            }
        }
        return $num;
    }

    public function stats() {
        return $this->client->statsTube($this->tube);
    }

    private function peekWith($method)
    {
        try {
            $data = $this->client->{$method}();
            $job = unserialize($data['body']);
            $message = new Message($job, $data['id']);

            if ($method == 'peekDelayed') {
                $stats = $this->client->statsJob($data['id']);
                $message->delayed = $stats['time-left'];
            }

            return $message;
        } catch (BeanstalkException $e) {
            if ($e->getMessage() == 'NOT_FOUND') {
                return null;
            } else {
                throw $e;
            }
        }
    }

    public static function isSupportReuseConnection()
    {
        return false;
    }

}
