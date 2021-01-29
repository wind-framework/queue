<?php

namespace Wind\Queue;

use Amp\Promise;
use Exception;
use Wind\Base\Config;
use Wind\Process\Process;
use Wind\Queue\Driver\ChanDriver;
use Wind\Queue\Driver\Driver;
use Psr\EventDispatcher\EventDispatcherInterface;
use function Amp\asyncCall;
use function Amp\call;

class ConsumerProcess extends Process
{

    protected $queue = 'default';

    /**
     * Queue Config
     *
     * @var array
     */
    private $config;

    /**
     * @var EventDispatcherInterface
     */
    private $eventDispatcher;

    public function __construct(Config $config, EventDispatcherInterface $eventDispatcher)
    {
        $this->name = 'QueueConsumer.'.$this->queue;

        $queueConfig = $config->get('queue.'.$this->queue);

        if (!$queueConfig) {
            throw new Exception("Unable to find queue '{$this->queue}' config.");
        }

        $this->count = $queueConfig['processes'] ?? 1;
        $this->config = $queueConfig;

        $this->eventDispatcher = $eventDispatcher;
    }

    public function run()
    {
        $concurrent = $this->config['concurrent'] ?? 1;

        //Use ChanDriver to optimize to connections.
        if (!empty($this->config['connection_optimize']) && $concurrent > 1) {
            $driver = new ChanDriver($this->config);
            for ($i=0; $i<$concurrent; $i++) {
                asyncCall([$this, 'createConsumer'], $driver, $i);
            }
            $driver->loop();
        } else {
            for ($i=0; $i<$concurrent; $i++) {
                $driver = new $this->config['driver']($this->config);
                asyncCall([$this, 'createConsumer'], $driver);
            }
        }
    }

    public function createConsumer(Driver $driver, $i=null)
    {
        yield $driver->connect();

        while (true) {
            $message = yield $driver->pop();

            //Allow pop timeout and return null to restart pop, like ping connection.
            if ($message === null) {
                continue;
            }

            /** @var Message $message */
            $job = $message->job;
            $jobClass = get_class($job);

            try {
                $this->eventDispatcher->dispatch(new QueueJobEvent(QueueJobEvent::STATE_GET, $jobClass, $message->id));
                yield call([$job, 'handle']);
                yield $driver->ack($message);
                $this->eventDispatcher->dispatch(new QueueJobEvent(QueueJobEvent::STATE_SUCCEED, $jobClass, $message->id));
            } catch (\Exception $e) {
                $attempts = $driver->attempts($message);
                ($attempts instanceof Promise) && $attempts = yield $attempts;

                if ($attempts < $message->job->maxAttempts) {
                    $this->eventDispatcher->dispatch(new QueueJobEvent(QueueJobEvent::STATE_ERROR, $jobClass, $message->id, $e));
                    yield $driver->release($message, $attempts+1);
                } else {
                    $this->eventDispatcher->dispatch(new QueueJobEvent(QueueJobEvent::STATE_FAILED, $jobClass, $message->id, $e));
                    if (yield call([$job, 'fail'], $message, $e)) {
                        yield $driver->fail($message);
                    } else {
                        yield $driver->delete($message->id);
                    }
                }
            }
        }
    }

}
