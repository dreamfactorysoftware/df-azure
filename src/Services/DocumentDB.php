<?php

namespace DreamFactory\Core\Azure\Services;

use DreamFactory\Core\Azure\Components\DocumentDBConnection;
use DreamFactory\Core\Azure\Database\Schema\DocumentDbSchema;
use DreamFactory\Core\Azure\Resources\DocumentDbTable;
use DreamFactory\Core\Database\Resources\DbSchemaResource;
use DreamFactory\Core\Database\Services\BaseDbService;

class DocumentDB extends BaseDbService
{
    /**
     * @var array
     */
    protected static $resources = [
        DbSchemaResource::RESOURCE_NAME => [
            'name'       => DbSchemaResource::RESOURCE_NAME,
            'class_name' => DbSchemaResource::class,
            'label'      => 'Schema',
        ],
        DocumentDbTable::RESOURCE_NAME  => [
            'name'       => DocumentDbTable::RESOURCE_NAME,
            'class_name' => DocumentDbTable::class,
            'label'      => 'Table',
        ],
    ];

    public function __construct($settings = [])
    {
        parent::__construct($settings);

        $uri = array_get($this->config, 'uri');
        $key = array_get($this->config, 'key');
        $database = array_get($this->config, 'database');
        $this->setConfigBasedCachePrefix($uri . $key . $database . ":");
    }

    protected function initializeConnection()
    {
        $uri = array_get($this->config, 'uri');
        $key = array_get($this->config, 'key');
        $database = array_get($this->config, 'database');

        if (empty($uri)) {
            throw new \InvalidArgumentException('Azure DocumentDB URI is missing. Check the service configuration.');
        }
        if (empty($key)) {
            throw new \InvalidArgumentException('Azure DocumentDB Key is missing. Check the service configuration.');
        }
        if (empty($database)) {
            throw new \InvalidArgumentException('Azure DocumentDB Database is missing. Check the service configuration.');
        }

        $this->dbConn = new DocumentDBConnection($uri, $key, $database);
        /** @noinspection PhpParamsInspection */
        $this->schema = new DocumentDbSchema($this->dbConn);
    }
}