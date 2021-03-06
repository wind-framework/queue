<?php

namespace Wind\Queue\Driver;

use Wind\Base\Chan;
use Wind\Queue\Message;
use function Amp\asyncCall;
use function Amp\Promise\all;

/**
 * A Queue Driver that only use two connection to serve unlimited consumers.
 *
 * Do not use this driver directly, use 'connection_optimize' config.
 *
 * @package Wind\Queue\Driver
 */
class ChanDriver implements Driver
{

    /**
     * @var Driver
     */
    private $popper;

    /**
     * @var Driver
     */
    private $operator;

    /**
     * @var Chan
     */
    private $chan;

    public function __construct($config)
    {
        $this->popper = new $config['driver']($config, true);
        $this->operator = new $config['driver']($config, true);

        $this->chan = new Chan();
    }

    public function connect()
    {
        return all([$this->popper->connect(), $this->operator->connect()]);
    }

    /**
     * @inheritDoc
     */
    public function push(Message $message, int $delay)
    {
        return $this->operator->push($message, $delay);
    }

    public function pop()
    {
        return $this->chan->receive();
    }

    public function loop()
    {
        asyncCall(function() {
            while ($receiver = yield $this->chan->getReceiver()) {
                $data = yield $this->popper->pop();
                $receiver->resolve($data);
            }
        });
    }

    public function ack(Message $message)
    {
        return $this->operator->ack($message);
    }

    public function fail(Message $message)
    {
        return $this->operator->fail($message);
    }

    public function release(Message $message, $delay)
    {
        return $this->operator->release($message, $delay);
    }

    /**
     * @inheritDoc
     */
    public function delete($id)
    {
        return $this->operator->delete($id);
    }

    public function attempts(Message $message)
    {
        return $this->operator->attempts($message);
    }

    public static function isSupportReuseConnection()
    {
        throw new \Exception('Do not user ChanDriver directly.');
    }

}