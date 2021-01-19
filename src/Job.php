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
	 * @return bool True into handle queue, or false delete queue
	 */
    public function fail()
    {
    	return true;
    }

}
