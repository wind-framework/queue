<?php

namespace Wind\Queue;

use Amp\Promise;
use Wind\Queue\Driver\Driver;
use function Amp\call;

class Queue
{

    //消息基础优先级
    const PRI_HIGH = 0;
    const PRI_NORMAL = 1;
    const PRI_LOW = 2;

    /**
     * @var Driver
     */
    private $driver;

    /**
     * @var Promise|null
     */
    private $connecting;

    public function __construct(Driver $driver)
    {
        $this->driver = $driver;
        $this->connecting = $driver->connect();
    }

    public function put(Job $job, $delay=0, $priority=self::PRI_NORMAL)
    {
        $message = new Message($job);
        $message->priority = $priority;
        return $this->call(function() use ($message, $delay) {
            return $this->driver->push($message, $delay);
        });
    }

    public function delete($id)
    {
        return $this->call(function() use ($id) {
            return $this->driver->delete($id);
        });
    }

    private function call($callback) {
        if ($this->connecting !== null) {
            return call(function() use ($callback) {
                yield $this->connecting;
                $this->connecting = null;
                return $callback();
            });
        } else {
            return $callback();
        }
    }

}