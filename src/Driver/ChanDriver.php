<?php

namespace Wind\Queue\Driver;

use Wind\Base\Chan;
use Wind\Queue\Message;

use function Amp\async;
use function Amp\await;
use function Amp\defer;

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
        return await([
            async([$this->popper, 'connect']),
            async([$this->operator, 'connect'])
        ]);
    }

    public function push(Message $message, int $delay)
    {
        return $this->operator->push($message, $delay);
    }

    public function pop()
    {
        return await($this->chan->receive());
    }

    public function loop()
    {
        defer(function() {
            while ($receiver = await($this->chan->getReceiver())) {
                $data = $this->popper->pop();
                $receiver->resolve($data);
            }
        });
    }

    public function ack(Message $message) { return $this->operator->ack($message); }

    public function fail(Message $message) { return $this->operator->fail($message); }

    public function release(Message $message, $delay)  { return $this->operator->release($message, $delay); }

    public function delete($id) { return $this->operator->delete($id); }

    public function attempts(Message $message) { return $this->operator->attempts($message); }

    public function peekDelayed() { return $this->operator->peekDelayed(); }

    public function peekReady() { return $this->operator->peekReady(); }

    public function peekFail() { return $this->operator->peekFail(); }

    public function wakeupJob($id) { return $this->operator->wakeup($id); }

    public function wakeup($num) { return $this->operator->wakeup($num); }

    public function drop($num) { return $this->operator->drop($num); }

    public function stats() { return $this->operator->stats(); }

    public static function isSupportReuseConnection()
    {
        throw new \Exception('Do not user ChanDriver directly.');
    }

}
