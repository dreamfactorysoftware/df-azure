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
        foreach ($collections as $collection){
            $tables[strtolower($collection)] = new TableSchema(['name' => $collection]);
        }

        return $tables;
    }

    /**
     * @inheritdoc
     */
    public function createTable($table, $schema, $options = null)
    {
        $data = ['id' => $table];
        if(isset($options['indexingPolicy'])){
            $data['indexingPolicy'] = $options['indexingPolicy'];
        }
        if(isset($options['partitionKey'])){
            $data['partitionKey'] = $options['partitionKey'];
        }

        return $this->connection->createCollection($data);
    }

    /**
     * @inheritdoc
     */
    protected function updateTable($table_name, $schema)
    {
        $data = ['id' => $table_name];
        if(isset($schema['indexingPolicy'])){
            $data['indexingPolicy'] = $schema['indexingPolicy'];
        }
        $this->connection->replaceCollection($data, $table_name);
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
