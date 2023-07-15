<?php

namespace Wind\Queue;

use Wind\Base\TouchableTimeoutCancellation;
use Wind\Queue\Driver\Driver;

class Message
{

    /**
     * 重试次数
     * @var int
     */
    public $attempts = 0;

    /**
     * 队列任务对象
     * @var Job
     */
    public $job;

    /**
     * 消息ID
     * @var string
     */
    public $id;

    /**
     * 消息原始对象
     * @var string|null
     */
    public $raw;

    /**
     * 优先级
     *
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
     * @var TouchableTimeoutCancellation|null
     */
    private $timeoutTouchable;

    /**
     * @var Driver
     */
    private $driver;

    public function __construct(Job $job, $id=null, $raw=null)
    {
        $this->job = $job;

        // Only attach message when pop from server
        if ($id !== null) {
            $this->id = $id;
            $this->job->attachMessage($this);
        }

        $raw && $this->raw = $raw;
    }

    public function setDriver(Driver $driver)
    {
        $this->driver = $driver;
    }

    public function setTouchable(TouchableTimeoutCancellation $timeoutTouchable)
    {
        $this->timeoutTouchable = $timeoutTouchable;
    }

    public function touch()
    {
        $this->timeoutTouchable?->touch();
        $this->driver?->touch($this);
    }

}
