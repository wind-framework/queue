<?php

namespace Wind\Queue\Driver;

use Wind\Beanstalk\BeanstalkClient;
use Wind\Beanstalk\BeanstalkException;
use Wind\Queue\Message;
use Wind\Queue\Queue;
use function Amp\call;

class BeanstalkDriver implements Driver
{

    /**
     * @var BeanstalkClient
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
        $this->client = new BeanstalkClient($config['host'], $config['port'], [
            'autoReconnect' => true,
            'concurrent' => true
        ]);

        //$this->client->debug = true;
        $this->tube = $config['tube'];

        if (isset($config['reserve_timeout']) && $config['reserve_timeout'] > 0) {
            $this->reserveTimeout = $config['reserve_timeout'];
        }
    }

    public function connect()
    {
        return call(function() {
            yield $this->client->connect();

            if ($this->tube != 'default') {
                yield $this->client->watch($this->tube);
                yield $this->client->ignore('default');
                yield $this->client->useTube($this->tube);
            }
        });
    }

    public function push(Message $message, $delay)
    {
        $raw = serialize($message->job);

        switch ($message->priority) {
            case Queue::PRI_NORMAL: $pri = BeanstalkClient::DEFAULT_PRI; break;
            case Queue::PRI_HIGH: $pri = 512; break;
            case Queue::PRI_LOW: $pri = 2048; break;
            default: $pri = $message->priority;
        }

        return $this->client->put($raw, $pri, $delay);
    }

    public function pop()
    {
        return call(function() {
            try {
                $data = yield $this->client->reserve($this->reserveTimeout);
                $job = unserialize($data['body']);
                return new Message($job, $data['id']);
            } catch (BeanstalkException $e) {
                switch ($e->getMessage()) {
                    case 'DEADLINE_SOON':
                    case 'TIMED_OUT': return null;
                    default: throw $e;
                }
            }
        });
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
        return call(function() use ($message, $delay) {
            $state = yield $this->client->statsJob($message->id);
            return $this->client->release($message->id, $state['pri'], $delay);
        });
    }

    public function attempts(Message $message)
    {
        return call(function() use ($message) {
            $state = yield $this->client->statsJob($message->id);
            return $state['releases'];
        });
    }

    public function delete($id)
    {
        return $this->client->delete($id);
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
        return call(function() use ($num) {
            for ($i=0; $i<$num; $i++) {
                try {
                    $data = yield $this->client->peekBuried();
                    yield $this->client->delete($data['id']);
                } catch (BeanstalkException $e) {
                    if ($e->getMessage() == 'NOT_FOUND') {
                        return $i;
                    } else {
                        throw $e;
                    }
                }
            }
            return $num;
        });
    }

    public function stats() {
        return $this->client->statsTube($this->tube);
    }

    private function peekWith($method)
    {
        return call(function() use ($method) {
            try {
                $data = yield $this->client->{$method}();
                $job = unserialize($data['body']);
                $message = new Message($job, $data['id']);

                if ($method == 'peekDelayed') {
                    $stats = yield $this->client->statsJob($data['id']);
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
        });
    }

    public static function isSupportReuseConnection()
    {
        return false;
    }

}
