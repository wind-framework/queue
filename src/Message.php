<?php

namespace Wind\Queue;

use Wind\Base\TouchableTimeoutToken;
use Wind\Queue\Driver\Driver;

use function Amp\call;

class Message
{

    /**
     * Retried times
     * @var int
     */
    public $attempts = 0;

    /**
     * Job object
     * @var Job
     */
    public $job;

    /**
     * Message Id
     * @var string
     */
    public $id;

    /**
     * Message raw
     * @var string|null
     */
    public $raw;

    /**
     * Priority
     * @var int
     */
    public $priority;

    /**
     * Delayed at timestamp
     *
     * @var int|null
     */
    public $delayed;

    /**
     * @var TouchableTimeoutToken|null
     */
    private $timeoutTouchable;

    /**
     * @var Driver
     */
    private $driver;

    public function __construct(Job $job, $id=null, $raw=null)
    {
        $this->job = $job;
        $this->job->attachMessage($this);
        $id && $this->id = $id;
        $raw && $this->raw = $raw;
    }

    public function setDriver(Driver $driver)
    {
        $this->driver = $driver;
    }

    public function setTouchable(TouchableTimeoutToken $timeoutTouchable)
    {
        $this->timeoutTouchable = $timeoutTouchable;
    }

    public function touch()
    {
        return call(function() {
            if ($this->timeoutTouchable) {
                $this->timeoutTouchable->touch();
            }
            if ($this->driver) {
                yield $this->driver->touch($this);
            }
        });
    }

}
