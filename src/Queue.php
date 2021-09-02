<?php

namespace Wind\Queue;

use RuntimeException;
use Wind\Queue\Driver\Driver;

/**
 * Queue Instance Class
 *
 * @method Message|null peekReady() Peek a ready message in queue
 * @method Message|null peekDelayed() Peek a delayed message in queue
 * @method Message|null peekFail() Peek a failed message in queue
 * @method int wakeup($num) Wakeup failed jobs to ready list by special numbers, return the real wakeup count.
 * @method int drop($num) Drop failed job by special numbers, return the real drop count.
 * @method bool delete($id) Delete the job by special job id
 * @method array stats() Get queue stats
 */
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

    public function __construct(Driver $driver)
    {
        $this->driver = $driver;
        $driver->connect();
    }

	/**
	 * Put job into queue
	 *
	 * @param Job $job The job to consume
	 * @param int $delay Delay time seconds, 0 mean no delay.
	 * @param int $priority The message priority, small number mean higher, big number mean lower.
	 * For safety, use PRI_ prefix constant is recommend, some driver (like RedisDriver)
	 * not support too much priority.
	 * @return int The Job id, you can delete the job use id before consume.
	 */
    public function put(Job $job, $delay=0, $priority=self::PRI_NORMAL)
    {
        $message = new Message($job);
        $message->priority = $priority;
        return $this->driver->push($message, $delay);
    }

    public function __call($name, $args)
    {
        static $methods = ['delete', 'stats', 'peekReady', 'peekDelayed', 'peekFail', 'wakeup', 'drop'];

        if (in_array($name, $methods)) {
            return call_user_func_array([$this->driver, $name], $args);
        } else {
            throw new RuntimeException("Call to undefined method ".__CLASS__."::$name()");
        }
    }

}
