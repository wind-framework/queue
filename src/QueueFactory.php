<?php

namespace Wind\Queue;

use Wind\Base\Config;

class QueueFactory
{

    /**
     * Queue instances
     * @var Queue[]
     */
    private $queues = [];

    public function get($queue='default')
    {
        if (isset($this->queues[$queue])) {
            return $this->queues[$queue];
        }

        $config = di()->get(Config::class)->get("queue.$queue");

        if (!$config) {
            throw new \InvalidArgumentException("Not found config for queue'$queue'.");
        }

        $config['use_single_instance'] = true;

        /** @var \Wind\Queue\Driver\Driver $driver */
        $driver = new $config['driver']($config);
        return $this->queues[$queue] = new Queue($driver);
    }

}
