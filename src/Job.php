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
     * Max attempts to consume job
     *
     * @var int
     */
    public $maxAttempts = 2;

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

}
