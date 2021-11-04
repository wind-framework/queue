<?php

namespace Wind\Queue\Driver;

use Wind\Db\Db;
use Wind\Db\DbException;
use Wind\Queue\Message;
use Wind\Queue\Queue;
use function Amp\call;
use function Amp\delay;

/*
Create the following table for queue database driver:

CREATE TABLE `wind_queue_channel` (
  `id` mediumint(8) unsigned NOT NULL AUTO_INCREMENT,
  `channel` char(20) NOT NULL,
  `total` bigint(20) unsigned DEFAULT '0',
  `last_time` int(10) unsigned DEFAULT '0',
  PRIMARY KEY (`id`),
  UNIQUE KEY `channel` (`channel`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `wind_queue_data` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `channel_id` mediumint(8) unsigned NOT NULL,
  `status` tinyint(1) unsigned NOT NULL DEFAULT '0' COMMENT '0=ready, 1=reserved, 2=fail',
  `job` mediumtext NOT NULL,
  `priority` tinyint(3) unsigned DEFAULT '128',
  `attempts` tinyint(3) unsigned DEFAULT '0',
  `created_at` int(10) unsigned DEFAULT NULL,
  `delayed` int(10) unsigned DEFAULT '0',
  PRIMARY KEY (`id`),
  KEY `channel_fetch` (`channel_id`,`status`,`delayed`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
*/

/**
 * Queue Database Driver
 */
class DbDriver implements Driver
{

    /**
     * @var string
     */
    private $channel;

    /**
     * @var int
     */
    private $channelId;

    /**
     * Database connection
     *
     * @var \Wind\Db\Connection
     */
    private $db;

    private $tableChannel;
    private $tableData;

    public function __construct($config)
    {
        $this->channel = $config['channel'];
        $this->db = isset($config['connection']) ? Db::connection($config['connection']) : Db::connection();
        $this->tableChannel = isset($config['table_channel']) ? $config['table_channel'] : 'queue_channel';
        $this->tableData = isset($config['table_data']) ? $config['table_data'] : 'queue_data';
    }

	public function connect() {
		return call(function() {
		    $channel = yield $this->db->table($this->tableChannel)->where(['channel'=>$this->channel])->fetchOne();

		    if (!$channel) {
		        yield $this->db->table($this->tableChannel)->insertIgnore(['channel'=>$this->channel]);
		        $channel = yield $this->db->table($this->tableChannel)->where(['channel'=>$this->channel])->fetchOne();
            }

		    if (!$channel) {
		        throw new DbException("DbDriver for queue error: Unable to get channel '{$this->channel}'.");
            }

		    $this->channelId = $channel['id'];
        });
	}

	/**
	 * @inheritDoc
	 */
	public function push(Message $message, int $delay) {
        return call(function() use ($message, $delay) {
            $data = serialize($message->job);

            switch ($message->priority) {
                case Queue::PRI_NORMAL: $pri = 128; break;
                case Queue::PRI_HIGH: $pri = 0; break;
                case Queue::PRI_LOW: $pri = 255; break;
                default: $pri = $message->priority;
            }

            $id = yield $this->db->table($this->tableData)->insert([
                'channel_id' => $this->channelId,
                'status' => 0,
                'job' => $data,
                'priority' => $pri,
                'created_at' => time(),
                'delayed' => $delay > 0 ? time() + $delay : 0
            ]);

            // Update will be block when pop..
            // yield $this->db->table($this->tableChannel)->where(['id'=>$this->channelId])->update([
            //     '^total' => 'total+1',
            //     'last_time' => time()
            // ]);

            return $id;
        });
	}

    /**
     * @inheritDoc
     */
	public function pop() {
	    return call(function() {
            /**
             * @var \Wind\Db\Transaction
             */
            $transaction = yield $this->db->beginTransaction();

            try {
                //Wait for channel
                $channel = yield $transaction->query("SELECT `id` FROM `".$this->db->prefix($this->tableChannel)."` WHERE `id`='{$this->channelId}' FOR UPDATE");

                if ($channel) {
                    //get message
                    $data = yield $transaction->table($this->tableData)
                        ->where(['channel_id'=>$this->channelId, 'status'=>0, 'delayed <'=>time()])
                        ->orderBy(['priority'=>SORT_ASC])
                        ->limit(1)
                        ->fetchOne();

                    if ($data) {
                        //update message status
                        yield $transaction->table($this->tableData)
                            ->where(['id'=>$data['id']])
                            ->update(['status'=>1]);

                        //unserialize job
                        $job = \unserialize($data['job']);
                        $message = new Message($job, $data['id']);
                        $message->attempts = $data['attempts'];
                        $message->priority = $data['priority'];

                        return $message;
                    }
                }

                yield delay(1000);

            } catch (\Throwable $e) {
                yield $transaction->commit();
            }
        });
	}

    /**
     * @inheritDoc
     */
	public function ack(Message $message) {
		return $this->delete($message->id);
	}

	public function fail(Message $message) {
		return $this->db->table($this->tableData)->where(['id'=>$message->id])->update(['status'=>2]);
	}

	/**
	 * @inheritDoc
	 */
	public function release(Message $message, $delay) {
		return $this->db->table($this->tableData)->where(['id'=>$message->id])->update(['status'=>0, 'delayed'=>$delay > 0 ? time() + $delay : 0]);
	}

	/**
	 * @inheritDoc
	 */
	public function attempts(Message $message) {
		return $message->attempts;
	}

	/**
	 * @inheritDoc
	 */
	public function delete($id) {
		return $this->db->table($this->tableData)->where(['id'=>$id])->delete();
	}

	/**
	 * @inheritDoc
	 */
	public function peekFail() {
		// TODO: Implement peekFail() method.
	}

	/**
	 * @inheritDoc
	 */
	public function peekDelayed() {
		// TODO: Implement peekDelayed() method.
	}

	/**
	 * @inheritDoc
	 */
	public function peekReady() {
		// TODO: Implement peekReady() method.
	}

	/**
	 * @inheritDoc
	 */
	public function wakeup($num) {
		// TODO: Implement wakeup() method.
	}

	/**
	 * @inheritDoc
	 */
	public function drop($num) {
		// TODO: Implement drop() method.
	}

	public function stats() {
		// TODO: Implement stats() method.
	}

	/**
	 * @inheritDoc
	 */
	public static function isSupportReuseConnection() {
		return true;
	}
}
