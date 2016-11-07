<?php

namespace DreamFactory\Core\Azure\Services;

use DreamFactory\Core\Azure\Components\DocumentDBConnection;
use DreamFactory\Core\Azure\Resources\DocumentDbSchema;
use DreamFactory\Core\Azure\Resources\DocumentDbTable;
use DreamFactory\Core\Database\Schema\TableSchema;
use DreamFactory\Core\Exceptions\InternalServerErrorException;
use DreamFactory\Core\Services\BaseNoSqlDbService;
use DreamFactory\Core\Components\DbSchemaExtras;
use DreamFactory\Core\Utility\Session;

class DocumentDB extends BaseNoSqlDbService
{
    use DbSchemaExtras;

    /** @var \DreamFactory\Core\Azure\Components\DocumentDBConnection|null  */
    protected $connection = null;

    /** @var array  */
    protected $tables = [];

    /**
     * @var array
     */
    protected static $resources = [
        DocumentDbSchema::RESOURCE_NAME => [
            'name'       => DocumentDbSchema::RESOURCE_NAME,
            'class_name' => DocumentDbSchema::class,
            'label'      => 'Schema',
        ],
        DocumentDbTable::RESOURCE_NAME  => [
            'name'       => DocumentDbTable::RESOURCE_NAME,
            'class_name' => DocumentDbTable::class,
            'label'      => 'Table',
        ],
    ];

    /** {@inheritdoc} */
    public function __construct(array $settings)
    {
        parent::__construct($settings);

        $config = (array)array_get($settings, 'config');
        Session::replaceLookups($config, true);

        $uri = array_get($config, 'uri');
        $key = array_get($config, 'key');
        $database = array_get($config, 'database');

        if(empty($uri)){
            throw new \InvalidArgumentException('Azure DocumentDB URI is missing. Check the service configuration.');
        }
        if(empty($key)){
            throw new \InvalidArgumentException('Azure DocumentDB Key is missing. Check the service configuration.');
        }
        if(empty($database)){
            throw new \InvalidArgumentException('Azure DocumentDB Database is missing. Check the service configuration.');
        }

        $this->connection = new DocumentDBConnection($uri, $key, $database);
    }

    /**
     * Destroys the connection
     */
    public function __destruct()
    {
        $this->connection = null;
    }

    /**
     * @return \DreamFactory\Core\Azure\Components\DocumentDBConnection|null
     * @throws \DreamFactory\Core\Exceptions\InternalServerErrorException
     */
    public function getConnection()
    {
        if (empty($this->connection)) {
            throw new InternalServerErrorException('Database connection has not been initialized.');
        }

        return $this->connection;
    }

    /** {@inheritdoc} */
    public function getTableNames($schema = null, $refresh = false, $use_alias = false)
    {
        if($refresh || (empty($this->tables) && null === $this->tables = $this->getFromCache('table_names'))){
            $tables = [];
            $collections = $this->connection->listCollections();
            foreach ($collections as $table){
                $tables[strtolower($table)] = new TableSchema(['name' => $table]);
            }
            // merge db extras
            if (!empty($extrasEntries = $this->getSchemaExtrasForTables($collections, false))) {
                foreach ($extrasEntries as $extras) {
                    if (!empty($extraName = strtolower(strval($extras['table'])))) {
                        if (array_key_exists($extraName, $tables)) {
                            $tables[$extraName]->fill($extras);
                        }
                    }
                }
            }
            $this->tables = $tables;
            $this->addToCache('table_names', $this->tables, true);
        }

        return $this->tables;
    }

    /** {@inheritdoc} */
    public function refreshTableCache()
    {
        $this->removeFromCache('table_names');
        $this->tables = [];
    }

    /** {@inheritdoc} */
    protected function handlePATCH()
    {
        return false;
    }
}