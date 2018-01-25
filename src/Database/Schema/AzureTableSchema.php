<?php
namespace DreamFactory\Core\Azure\Database\Schema;

use DreamFactory\Core\Database\Components\Schema;
use DreamFactory\Core\Database\Schema\ColumnSchema;
use DreamFactory\Core\Database\Schema\TableSchema;
use MicrosoftAzure\Storage\Table\Models\QueryTablesResult;
use MicrosoftAzure\Storage\Table\TableRestProxy;

/**
 * Schema is the class for retrieving metadata information from a MongoDB database (version 4.1.x and 5.x).
 */
class AzureTableSchema extends Schema
{
    /**
     * @var TableRestProxy
     */
    protected $connection;

    /**
     * @inheritdoc
     */
    protected function loadTableColumns(TableSchema $table)
    {
        $table->primaryKey = ['PartitionKey','RowKey'];
        $c = new ColumnSchema([
            'name'           => 'PartitionKey',
            'db_type'        => 'string',
            'is_primary_key' => true,
            'auto_increment' => true,
        ]);
        $c->quotedName = $this->quoteColumnName($c->name);
        $table->addColumn($c);
        $c = new ColumnSchema([
            'name'           => 'RowKey',
            'db_type'        => 'string',
            'is_primary_key' => true,
            'auto_increment' => true,
        ]);
        $c->quotedName = $this->quoteColumnName($c->name);
        $table->addColumn($c);
    }

    /**
     * @inheritdoc
     */
    protected function getTableNames($schema = '')
    {
        $tables = [];
        /** @var QueryTablesResult $result */
        $result = $this->connection->queryTables();
        $names = $result->getTables();
        foreach ($names as $name) {
            $tables[strtolower($name)] = new TableSchema(['name' => $name]);
        }

        return $tables;
    }

    /**
     * @inheritdoc
     */
    public function createTable($table, $options)
    {
        if (empty($tableName = array_get($table, 'name'))) {
            throw new \Exception("No valid name exist in the received table schema.");
        }

        if (!empty($native = array_get($table, 'native'))) {

        }

        return $this->connection->createTable($tableName);
    }

    /**
     * @inheritdoc
     */
    public function updateTable($tableSchema, $changes)
    {
        // nothing to do here
    }

    /**
     * @inheritdoc
     */
    public function dropTable($table)
    {
        $this->connection->deleteTable($table);

        return 0;
    }

    /**
     * @inheritdoc
     */
    public function createFieldReferences($references)
    {
        // Do nothing here for now
    }

    /**
     * @inheritdoc
     */
    public function createFieldIndexes($indexes)
    {
        // Do nothing here for now
    }
}
