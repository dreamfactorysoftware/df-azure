<?php
namespace DreamFactory\Core\Azure\Database\Schema;

use DreamFactory\Core\Database\Components\Schema;
use DreamFactory\Core\Database\Schema\ColumnSchema;
use DreamFactory\Core\Database\Schema\TableSchema;
use Illuminate\Support\Arr;

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
    protected function loadTableColumns(TableSchema $table)
    {
        $table->native = $this->connection->getCollection($table->name);
        $table->addPrimaryKey('id');
        $c = new ColumnSchema([
            'name'           => 'id',
            'db_type'        => 'string',
            'is_primary_key' => true,
            'auto_increment' => false,
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
        $collections = $this->connection->listCollections();
        foreach ($collections as $name) {
            $tables[strtolower($name)] = new TableSchema(['name' => $name]);
        }

        return $tables;
    }

    /**
     * @inheritdoc
     */
    public function createTable($table, $options)
    {
        if (empty($tableName = Arr::get($table, 'name'))) {
            throw new \Exception("No valid name exist in the received table schema.");
        }

        $data = ['id' => $tableName];
        if (!empty($native = Arr::get($table, 'native'))) {
            if (isset($native['indexingPolicy'])) {
                $data['indexingPolicy'] = $native['indexingPolicy'];
            }
            if (isset($native['partitionKey'])) {
                $data['partitionKey'] = $native['partitionKey'];
            }
        }

        return $this->connection->createCollection($data);
    }

    /**
     * @inheritdoc
     */
    public function updateTable($tableSchema, $changes)
    {
        $data = ['id' => $tableSchema->quotedName];
        if (!empty($native = Arr::get($changes, 'native'))) {
            if (isset($native['indexingPolicy'])) {
                $data['indexingPolicy'] = $native['indexingPolicy'];
            }
        }

        $this->connection->replaceCollection($data, $tableSchema->quotedName);
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
