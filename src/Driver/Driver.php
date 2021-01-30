<?php

namespace Wind\Queue\Driver;

use Amp\Promise;
use Wind\Queue\Message;

interface Driver
{

    public function connect();

    /**
     * 放入消息
     *
     * @param Message $message
     * @param int $delay 消息延迟秒数，0代表不延迟
     * @return string|int
     */
    public function push(Message $message, int $delay);

    public function pop();

    public function ack(Message $message);

    public function fail(Message $message);

    public function release(Message $message, $delay);

    /**
     * 获取消息的已尝试次数
     *
     * @param Message $message
     * @return int|Promise<int>
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
