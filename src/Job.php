<?php

namespace Wind\Queue;

use function Amp\call;

/**
 * Queue Job
 */
abstract class Job
{

    /**
     * Time to run (seconds)
     *
     * @var int
     */
    public $ttr = 60;

    /**
     * Max attempts to consume job
     *
     * @var int
     */
    public $maxAttempts = 2;

    /**
     * @var Message
     */
    private $message;

    abstract public function handle();

	/**
	 * Handle before job into fail queue
	 *
	 * @param Message $message Message object
	 * @param \Throwable $ex The last failed exception
	 * @return bool True into handle queue, or false delete queue
	 */
    public function fail($message, $ex)
    {
    	return true;
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
            $names[] = $p->getName();
        }

        return $names;
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
     * Touch current job to re-calculate ttr time
     *
     * @return \Amp\Promise
     */
    final public function touch()
    {
        return call(function() {
            if ($this->message) {
                yield $this->message->touch();
            } else {
                throw new QueueException('Touch failed, message not set.');
            }
        });
    }

}
