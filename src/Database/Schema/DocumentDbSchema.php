<?php
namespace DreamFactory\Core\Azure\Database\Schema;

use DreamFactory\Core\Database\Schema\Schema;
use DreamFactory\Core\Database\Schema\TableSchema;
use DreamFactory\Core\Enums\DbSimpleTypes;

/**
 * Schema is the class for retrieving metadata information from a MongoDB database (version 4.1.x and 5.x).
 */
class DocumentDbSchema extends Schema
{
    /**
     * @var \DreamFactory\Core\Azure\Components\DocumentDBConnection|null
     */
    protected $connection;

    /**
     * @inheritdoc
     */
    protected function findColumns(TableSchema $table)
    {
        $table->native = $this->connection->getCollection($table->name);
        $columns = [
            [
                'name'           => 'id',
                'db_type'        => 'string',
                'is_primary_key' => true,
                'auto_increment' => false,
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
        $collections = $this->connection->listCollections();
        foreach ($collections as $collection) {
            $tables[strtolower($collection)] = new TableSchema(['name' => $collection]);
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

        $data = ['id' => $tableName];
        if (!empty($native = array_get($table, 'native'))) {
            if (isset($options['indexingPolicy'])) {
                $data['indexingPolicy'] = $native['indexingPolicy'];
            }
            if (isset($options['partitionKey'])) {
                $data['partitionKey'] = $native['partitionKey'];
            }
        }

        return $this->connection->createCollection($data);
    }

    /**
     * @inheritdoc
     */
    protected function updateTable($table, $changes)
    {
        if (empty($tableName = array_get($table, 'name'))) {
            throw new \Exception("No valid name exist in the received table schema.");
        }

        $data = ['id' => $tableName];
        if (!empty($native = array_get($table, 'native'))) {
            if (isset($native['indexingPolicy'])) {
                $data['indexingPolicy'] = $native['indexingPolicy'];
            }
        }

        $this->connection->replaceCollection($data, $table);
    }

    /**
     * @inheritdoc
     */
    public function dropTable($table)
    {
        return $this->connection->deleteCollection($table);
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
}
