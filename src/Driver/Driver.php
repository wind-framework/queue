<?php

namespace Wind\Queue\Driver;

use Wind\Queue\Message;

interface Driver
{

    /**
     * 放入消息
     *
     * @param Message $message
     * @param int $delay 消息延迟秒数，0代表不延迟
     * @return string|int
     */
    public function push(Message $message, int $delay);

    /**
     * Pop a message
     *
     * @return Message|null
     */
    public function pop();

    public function ack(Message $message);

    public function fail(Message $message);

    /**
     * Release reserved job to ready list
     *
     * @param Message $message
     * @param int $delay
     * @return void
     */
    public function release(Message $message, $delay);

    /**
     * 获取消息的已尝试次数
     *
     * @param Message $message
     * @return int
     */
    public function attempts(Message $message);

    /**
     * 删除消息
     *
     * @param string $id
     * @return bool
     */
    public function delete($id);

    /**
     * 重置任务超时时间以允许执行更久
     *
     * @param Message $message
     */
    public function touch(Message $message);

    /**
     * 预览失败列表中的一条消息
     *
     * @return Message
     */
    public function peekFail();

    /**
     * 预览延迟列表中的一条消息
     *
     * @return Message
     */
    public function peekDelayed();

    /**
     * 预览准备列表中的一条消息
     *
     * @return Message
     */
    public function peekReady();

    /**
     * Wakeup failed jobs to ready list
     *
     * @param int $num Number of jobs to wakeup
     * @return int
     */
    public function wakeup($num);

    /**
     * Drop failed jobs
     *
     * @param int $num
     * @return int
     */
    public function drop($num);

    /**
     * Get queue statistics data
     *
     * @return array An array that include fields:
     * fails: Count failed jobs in current queue,
     * ready: Count ready jobs that wait to consume,
     * delayed: Count delayed job that will move to ready in future,
     * reserved: Count reserved and processing jobs,
     * total_jobs: All jobs count,
     * server: Server information, eg: Server name, version, OS,
     * uptime: Queue server uptime seconds.
     */
    public function stats();

    /**
     * 驱动是否支持连接复用
     *
     * 如果驱动支持连接重用，则系统会使用 ChanDriver 嵌套该驱动，从而实现仅用两个连接在一个进程内支持无数的消费者协程。
     * 并不所有驱动都支持连接复用，比如 Beanstalkd 通过 reserve 获得的消息只有该连接可以 delete 消息。
     * 所以要确认支持时才返回 true，否则默认请返回 false
     *
     * @return bool
     */
    public static function isSupportReuseConnection();

}
