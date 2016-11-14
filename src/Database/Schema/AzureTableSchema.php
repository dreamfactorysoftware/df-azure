<?php
namespace DreamFactory\Core\Azure\Database\Schema;

use DreamFactory\Core\Database\Schema\Schema;
use DreamFactory\Core\Database\Schema\TableSchema;
use DreamFactory\Core\Enums\DbSimpleTypes;
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
    protected function findColumns(TableSchema $table)
    {
        $columns = [
            [
                'name'           => 'PartitionKey',
                'db_type'        => 'string',
                'is_primary_key' => true,
                'auto_increment' => true,
            ],
            [
                'name'           => 'RowKey',
                'db_type'        => 'string',
                'is_primary_key' => true,
                'auto_increment' => true,
            ]
        ];

        return $columns;
    }

    /**
     * @inheritdoc
     */
    protected function findTableNames($schema = '', $include_views = true)
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
    public function createTable($table, $schema, $options = null)
    {
        return $this->connection->createTable($table, $options);
    }

    /**
     * @inheritdoc
     */
    protected function updateTable($table_name, $schema)
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
    protected function createFieldReferences($references)
    {
        // Do nothing here for now
    }

    /**
     * @inheritdoc
     */
    protected function createFieldIndexes($indexes)
    {
        // Do nothing here for now
    }

    /**
     * @inheritdoc
     */
    public function parseValueForSet($value, $field_info)
    {
        switch ($field_info->type) {
            case DbSimpleTypes::TYPE_BOOLEAN:
                $value = (filter_var($value, FILTER_VALIDATE_BOOLEAN) ? 1 : 0);
                break;
        }

        return $value;
    }
}
