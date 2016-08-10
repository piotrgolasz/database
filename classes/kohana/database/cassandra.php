<?php

defined('SYSPATH') OR die('No direct script access.');

include Kohana::find_file('vendor/php-cassandra/vendor', 'autoload');

class Kohana_Database_Cassandra extends Database {

	public function begin($mode = NULL)
	{
		
	}

	public function commit()
	{
		
	}

	public function connect()
	{
		if ($this->_connection)
		{
			return;
		}

		try
		{
			$this->_connection = new \Cassandra\Connection($this->_config['connection']['nodes'], $this->_config['connection']['database']);

			if (!$this->_connection->isConnected())
			{
				throw new Database_Exception('Can\'t connect to any Cassandra nodes');
			}
		}
		catch (Exception $ex)
		{
			throw new Kohana_Exception($ex->getMessage(), NULL, $ex->getCode());
		}
	}

	public function escape($value)
	{
		
	}

	public function list_columns($table, $like = NULL, $add_prefix = TRUE)
	{
		// Quote the table name
		//$table = ($add_prefix === TRUE) ? $this->quote_table($table) : $table;

		if (is_string($like))
		{
			// Search for column names
			$result = $this->query(Database::SELECT, 'SELECT * FROM system_schema.columns where keyspace_name=\'' . $this->_config['connection']['database'] . '\' and table_name=\'' . $table . '\' and column_name=\'' . $like . '\'', FALSE);
		}
		else
		{
			// Find all column names
			$result = $this->query(Database::SELECT, 'SELECT * FROM system_schema.columns where keyspace_name=\'' . $this->_config['connection']['database'] . '\' and table_name=\'' . $table . '\'', FALSE);
		}

		$count = 0;
		$columns = array();

		foreach ($result as $row)
		{
			list($type, $length) = $this->_parse_type($row['0']['type']);

			$column = $this->datatype($type);

			$column['column_name'] = $row['0']['column_name'];
			$column['column_default'] = NULL;
			$column['data_type'] = $type;
			$column['is_nullable'] = TRUE;
			$column['ordinal_position'] = ++$count;

			switch ($column['type']) {
				case 'float':
					if (isset($length))
					{
						list($column['numeric_precision'], $column['numeric_scale']) = explode(', ', $length);
					}
					break;
				case 'int':
					if (isset($length))
					{
						// MySQL attribute
						$column['display'] = $length;
					}
					break;
				case 'string':
					switch ($column['data_type']) {
						case 'binary':
						case 'varbinary':
							$column['character_maximum_length'] = $length;
							break;
						case 'char':
						case 'varchar':
							$column['character_maximum_length'] = $length;
						case 'text':
						case 'tinytext':
						case 'mediumtext':
						case 'longtext':
							$column['collation_name'] = 'utf8_general_ci';
							break;
						case 'enum':
						case 'set':
							$column['collation_name'] = 'utf8_general_ci';
							$column['options'] = explode('\',\'', substr($length, 1, -1));
							break;
					}
					break;
			}
		}

		var_dump(__FILE__ . '' . __LINE__);
		var_dump($columns);
		die();
		return $columns;
	}

	public function list_tables($like = NULL)
	{
		
	}

	public function query($type, $sql, $as_object = FALSE, array $params = NULL)
	{
		$this->_connection OR $this->connect();

		if (!empty($this->_config['profiling']))
		{
			// Benchmark this query for the current instance
			$benchmark = Profiler::start("Database ({$this->_instance})", $sql);
		}

		$result = $this->_connection->querySync($sql)->getData();

		if (isset($benchmark))
		{
			Profiler::stop($benchmark);
		}

		// Set the last query
		$this->last_query = $sql;

		return new Database_Cassandra_Result((Array) $result, $sql, $as_object, $params);
	}

	public function rollback()
	{
		
	}

	public function set_charset($charset)
	{
		
	}

//put your code here
}
