<?php

namespace Wind\Queue;

abstract class Job
{

    /**
     * Default TTR
     *
     * @var int
     */
    public $ttr = 60;

    /**
     * Max number of retries to consume job
     *
     * @var int
     */
    public $maxRetries  = 2;

    /**
     * Attempt interval seconds
     *
     * @var int|array
     */
    public $retryInterval = null;

    /**
     * @var Message
     */
    private $message;

    abstract public function handle();

	/**
	 * Handle before job into fail queue
	 *
	 * @param Message $message Message object
	 * @param \Exception $ex The last failed exception
	 * @return bool True into handle queue, or false delete queue
	 */
    public function fail($message, $ex)
    {
    	return true;
    }

    /**
     * Retry seconds
     *
     * @param int $attempts Current attempt times, start from zero.
     * @return int Current attempt delay seconds
     */
    public function retrySeconds($attempts)
    {
        if (isset($this->retryInterval)) {
            if (is_array($this->retryInterval)) {
                return $this->retryInterval[$attempts] ?? end($this->retryInterval);
            } else {
                return $this->retryInterval;
            }
        } else {
            return ($attempts + 1) * 5;
        }
    }

    /**
     * Set job message
     */
    final public function attachMessage($message)
    {
        if ($this->message === null) {
            $this->message = $message;
        } else {
            throw new QueueException('Message is already been set.');
        }
    }

    /**
     * Touch current job to reset ttr timer
     *
     * @return \Amp\Promise
     */
    final public function touch()
    {
        if ($this->message) {
            $this->message->touch();
        } else {
            throw new QueueException('Touch failed, message not set.');
        }
    }

    /**
     * Only serialize properties without private
     * @return array
     */
    public function __sleep()
    {
        $ref = new \ReflectionClass(static::class);
        $props = $ref->getProperties(\ReflectionProperty::IS_PUBLIC | \ReflectionProperty::IS_PROTECTED);
        $names = [];

        foreach ($props as $p) {
            $name = $p->getName();
            //don't serialize retryInterval if its not set
            if ($name == 'retryInterval' && $this->$name === null) {
                continue;
            }
            $names[] = $name;
        }

        return $names;
    }

}
