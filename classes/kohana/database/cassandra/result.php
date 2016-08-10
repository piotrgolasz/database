<?php

defined('SYSPATH') OR die('No direct script access.');

class Kohana_Database_Cassandra_Result extends Database_Result {

	protected $_internal_row = 0;

	public function __construct($result, $sql, $as_object = FALSE, array $params = NULL)
	{
		parent::__construct($result, $sql, $as_object, $params);

		// Find the number of rows in the result
		$this->_total_rows = count($result);
	}

	public function __destruct()
	{
		if (is_resource($this->_result))
		{
			unset($this->_result);
		}
	}

	public function current()
	{
		if ($this->_current_row !== $this->_internal_row AND ! $this->seek($this->_current_row))
		{
			return NULL;
		}

		// Increment internal row for optimization assuming rows are fetched in order
		$this->_internal_row++;

		if ($this->_as_object === TRUE)
		{
			// Return an stdClass
			$object = new stdClass();
			foreach ($this->_result as $key => $value)
			{
				$object->{$key} = $value;
			}
			return $object;
		}
		elseif (is_string($this->_as_object))
		{
			// Return an object of given class name
			$object = new $this->_as_object();
			foreach ($this->_result as $key => $value)
			{
				$object->{$key} = $value;
			}
			return $object;
		}
		else
		{
			// Return an array of the row
			return $this->_result;
		}
	}

	public function seek($position)
	{
		
	}

//put your code here
}
