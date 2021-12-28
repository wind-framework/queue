<?php

namespace Wind\Queue\Driver;

use Wind\Db\Db;
use Wind\Db\DbException;
use Wind\Db\QueryBuilder;
use Wind\Queue\Message;
use Wind\Queue\Queue;
use function Amp\call;
use function Amp\delay;

/*
Create the following table for queue database driver:

CREATE TABLE `wind_queue_sync` (
	`id` MEDIUMINT(8) UNSIGNED NOT NULL AUTO_INCREMENT,
	`channel` CHAR(128) NOT NULL COLLATE 'utf8mb4_general_ci',
	PRIMARY KEY (`id`),
	UNIQUE INDEX `channel` (`channel`)
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

    private $tableSync = 'sync';
    private $tableData = 'data';

    /**
     * Increase idle seconds
     *
     * When no message pop, the next pop loop will be delay by $idleCurrent seconds,
     * and $idleCurrent will be continue increase when there is no message, and max by $idleMax.
     * When there is message arrived, $idleCurrent will be reset to zero.
     *
     * @var float
     */
    private $idleIncrease = 0.5;

    /**
     * Max idle seconds when no pop message.
     *
     * @var float
     */
    private $idleMax = 5;

    /**
     * Current idle seconds
     *
     * @var float
     */
    private $idleCurrent = 0;

    private const STATUS_READY = 0;
    private const STATUS_RESERVED = 1;
    private const STATUS_FAIL = 2;

    public function __construct($config)
    {
        $this->channel = $config['channel'];
        $this->db = isset($config['connection']) ? Db::connection($config['connection']) : Db::connection();

        if (isset($config['table_prefix'])) {
            $this->tableSync = $config['table_prefix'].$this->tableSync;
            $this->tableData = $config['table_prefix'].$this->tableData;
        }

        isset($config['idle_increase']) && $this->idleIncrease = $config['idle_increase'];
        isset($config['idle_max']) && $this->idleMax = $config['idle_max'];
    }

	public function connect() {
		return call(function() {
		    $channel = yield $this->db->table($this->tableSync)->where(['channel'=>$this->channel])->fetchOne();

		    if (!$channel) {
		        yield $this->db->table($this->tableSync)->insertIgnore(['channel'=>$this->channel]);
		        $channel = yield $this->db->table($this->tableSync)->where(['channel'=>$this->channel])->fetchOne();
            }

		    if (!$channel) {
		        throw new DbException("DbDriver for queue error: Unable to get channel '{$this->channel}'.");
            }

		    $this->channelId = $channel['id'];
        });
	}

    /**
     * Get Data Table QueryBuilder
     *
     * @return QueryBuilder
     */
    private function data()
    {
        return $this->db->table($this->tableData);
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

            $id = yield $this->data()->insert([
                'channel_id' => $this->channelId,
                'job' => $data,
                'priority' => $pri,
                'created_at' => time(),
                'delayed' => $delay > 0 ? time() + $delay : 0
            ]);

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
                yield $transaction->query("SELECT `id` FROM `".$this->db->prefix($this->tableSync)."` WHERE `id`='{$this->channelId}' FOR UPDATE");

                //get message
                $data = yield $transaction->table($this->tableData)
                    ->where(['channel_id'=>$this->channelId, 'status'=>self::STATUS_READY, 'delayed <='=>time()])
                    ->orderBy(['priority'=>SORT_ASC])
                    ->limit(1)
                    ->fetchOne();

                if ($data) {
                    //update message status
                    yield $transaction->table($this->tableData)
                        ->where(['id'=>$data['id']])
                        ->update(['status'=>1]);

                    yield $transaction->commit();

                    //unserialize job
                    $job = \unserialize($data['job']);
                    $message = new Message($job, $data['id']);
                    $message->attempts = $data['attempts'];
                    $message->priority = $data['priority'];

                    $this->idleCurrent = 0;

                    return $message;
                }

                yield $transaction->commit();

                // Step increase idle time to decrease cpu/query usage
                if ($this->idleCurrent > 0) {
                    yield delay($this->idleCurrent * 1000);
                }

                if ($this->idleCurrent < $this->idleMax) {
                    $nextIdle = $this->idleCurrent + $this->idleIncrease;
                    $this->idleCurrent = $nextIdle < $this->idleMax ? $nextIdle : $this->idleMax;
                }

            } catch (\Throwable $e) {
                yield $transaction->rollback();
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
		return $this->data()->where(['id'=>$message->id])->update(['status'=>2]);
	}

	/**
	 * @inheritDoc
	 */
	public function release(Message $message, $delay) {
		return $this->data()->where(['id'=>$message->id])->update(['status'=>self::STATUS_READY, '^attempts'=>'attempts+1', 'delayed'=>$delay > 0 ? time() + $delay : 0]);
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
		return $this->data()->where(['id'=>$id])->delete();
	}

    private function messageByData($data)
    {
        $job = \unserialize($data['job']);
        $message = new Message($job, $data['id']);
        $message->attempts = $data['attempts'];
        $message->priority = $data['priority'];
        $delayed = $data['delayed'] - time();
        $message->delayed = $delayed > 0 ? $delayed : 0;
        return $message;
    }

	/**
	 * @inheritDoc
	 */
	public function peekFail() {
		return call(function() {
            $data = yield $this->data()->where(['channel_id'=>$this->channelId, 'status'=>self::STATUS_FAIL])->limit(1)->fetchOne();
            return $data ? $this->messageByData($data) : null;
        });
	}

	/**
	 * @inheritDoc
	 */
	public function peekDelayed() {
		return call(function() {
            $data = yield $this->data()
                ->where(['channel_id'=>$this->channelId, 'status'=>self::STATUS_READY, 'delayed >'=>time()])
                ->orderBy(['delayed'=>SORT_ASC, 'priority'=>SORT_ASC])
                ->limit(1)
                ->fetchOne();
            return $data ? $this->messageByData($data) : null;
        });
	}

	/**
	 * @inheritDoc
	 */
	public function peekReady() {
		return call(function() {
            $data = yield $this->data()
                ->where(['channel_id'=>$this->channelId, 'status'=>self::STATUS_READY, 'delayed <='=>time()])
                ->orderBy(['priority'=>SORT_ASC])
                ->limit(1)
                ->fetchOne();
            return $data ? $this->messageByData($data) : null;
        });
	}

	/**
	 * @inheritDoc
	 */
	public function wakeup($num) {
		return $this->data()->where(['channel_id'=>$this->channelId, 'status'=>self::STATUS_FAIL])->limit($num)->update(['status'=>self::STATUS_READY]);
	}

	/**
	 * @inheritDoc
	 */
	public function drop($num) {
		return $this->data()->where(['channel_id'=>$this->channelId, 'status'=>self::STATUS_FAIL])->limit($num)->delete();
	}

	public function stats() {
        return call(function() {
            $cc = yield $this->data()
                ->select('status,COUNT(*) as count')
                ->where(['channel_id'=>$this->channelId, 'status'=>[self::STATUS_FAIL, self::STATUS_RESERVED]])
                ->groupBy('status')
                ->indexBy('status')
                ->fetchColumn('count');

            $info = yield $this->db->fetchOne('SHOW TABLE STATUS WHERE `name`=\''.$this->db->prefix($this->tableData).'\'');

            $data = [
                'fails' => $cc[self::STATUS_FAIL] ?? 0,
                'ready' => yield $this->data()->where(['channel_id'=>$this->channelId, 'status'=>self::STATUS_READY, 'delayed <='=>time()])->count() ?: 0,
                'delayed' => yield $this->data()->where(['channel_id'=>$this->channelId, 'status'=>self::STATUS_READY, 'delayed >'=>time()])->count() ?: 0,
                'reserved' => $cc[self::STATUS_RESERVED] ?? 0,
                'total_jobs' => $info['Auto_increment'] - 1
            ];

            $type = $this->db->getType();
            $versions = yield $this->db->indexBy('Variable_name')->fetchColumn('SHOW VARIABLES LIKE \'version%\'', [], 'Value');

            $servers = ['MySQL']; //Multi DB driver detect
            $server = $type;

            foreach ($servers as $s) {
                if (stripos($s, $type) !== false) {
                    $server = $s;
                    break;
                }
            }

            $data['server'] = "$server {$versions['version']} {$versions['version_comment']} ({$versions['version_compile_os']} {$versions['version_compile_machine']})";

            $uptime = yield $this->db->fetchOne('SHOW GLOBAL STATUS LIKE \'uptime\'');
            $data['uptime'] = $uptime['Value'];

            return $data;
        });
	}

	/**
	 * @inheritDoc
	 */
	public static function isSupportReuseConnection() {
		return true;
	}
}
