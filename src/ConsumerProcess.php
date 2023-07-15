<?php

namespace Wind\Queue;

use Exception;
use Psr\EventDispatcher\EventDispatcherInterface;
use Wind\Base\Config;
use Wind\Base\TouchableTimeoutCancellation;
use Wind\Process\Process;
use Wind\Queue\Driver\ChanDriver;
use Wind\Queue\Driver\Driver;
use Wind\Process\Stateful;

use function Amp\async;

class ConsumerProcess extends Process
{

    use Stateful;

    /**
     * Queue name
     *
     * @var string
     */
    protected $queue = 'default';

    /**
     * Queue Config
     *
     * @var array
     */
    private $config;

    private EventDispatcherInterface $eventDispatcher;

    /**
     * 当前工作协程状态
     *
     * 键代表协程的编号，值为当前正在处理的任务，值为 null 时代表协程空闲状态
     *
     * @var array
     */
    private $concurrentState = [];

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
        if ($concurrent > 2 && $this->config['driver']::isSupportReuseConnection()) {
            $driver = new ChanDriver($this->config);
            for ($i=0; $i<$concurrent; $i++) {
                defer(fn() => $this->createConsumer($i, $driver));
            }
            $driver->loop();
        } else {
            for ($i=0; $i<$concurrent; $i++) {
                $driver = new $this->config['driver']($this->config);
                defer(fn() => $this->createConsumer($i, $driver));
            }
        }
    }

    public function createConsumer($num, Driver $driver)
    {
        while (true) {
            $this->concurrentState[$num] = null;

            $message = $driver->pop();

            //Allow pop timeout and return null to restart pop, like ping connection.
            if ($message === null) {
                continue;
            }

            $job = $message->job;
            $jobClass = get_class($job);

            //Mark concurrent state
            $this->concurrentState[$num] = ['id'=>$message->id, 'job'=>$jobClass];

            try {
                $this->eventDispatcher->dispatch(new QueueJobEvent(QueueJobEvent::STATE_GET, $jobClass, $message->id));

                $touchable = new TouchableTimeoutCancellation($job->ttr, "Job consume {$message->id} consume timeout.");
                $message->setTouchable($touchable);
                $message->setDriver($driver);

                async(static fn() => $job->handle())->await($touchable);
                $driver->ack($message);

                $this->eventDispatcher->dispatch(new QueueJobEvent(QueueJobEvent::STATE_SUCCEED, $jobClass, $message->id));

            } catch (\Throwable $e) {
                $attempts = $driver->attempts($message);

                if ($attempts < $message->job->maxAttempts) {
                    $this->eventDispatcher->dispatch(new QueueJobEvent(QueueJobEvent::STATE_ERROR, $jobClass, $message->id, $e));
                    $driver->release($message, $attempts+1);
                } else {
                    $this->eventDispatcher->dispatch(new QueueJobEvent(QueueJobEvent::STATE_FAILED, $jobClass, $message->id, $e));
                    if ($job->fail($message, $e)) {
                        $driver->fail($message);
                    } else {
                        $driver->delete($message->id);
                    }
                }
            }
        }
    }

    public function getState() {
        return [
            'type' => 'queue_consumer_concurrent',
            'group' => $this->queue,
            'stat' => $this->concurrentState
        ];
    }

}
