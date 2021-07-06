<?php

namespace Wind\Queue;

use Amp\Promise;
use Exception;
use Wind\Base\Config;
use Wind\Process\Process;
use Wind\Queue\Driver\ChanDriver;
use Wind\Queue\Driver\Driver;
use Psr\EventDispatcher\EventDispatcherInterface;
use Wind\Process\Stateful;

use function Amp\asyncCall;
use function Amp\call;

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

    /**
     * @var EventDispatcherInterface
     */
    private $eventDispatcher;

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
            yield $driver->connect();
            for ($i=0; $i<$concurrent; $i++) {
                asyncCall([$this, 'createConsumer'], $i, $driver, false);
            }
            $driver->loop();
        } else {
            for ($i=0; $i<$concurrent; $i++) {
                $driver = new $this->config['driver']($this->config);
                asyncCall([$this, 'createConsumer'], $i, $driver, true);
            }
        }
    }

    public function createConsumer($num, Driver $driver, $connectInConsumer)
    {
        if ($connectInConsumer) {
            yield $driver->connect();
        }

        while (true) {
            $this->concurrentState[$num] = null;

            /** @var Message $message */
            $message = yield $driver->pop();

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

    public function getState() {
        return [
            'type' => 'queue_consumer_concurrent',
            'group' => $this->queue,
            'stat' => $this->concurrentState
        ];
    }

}
