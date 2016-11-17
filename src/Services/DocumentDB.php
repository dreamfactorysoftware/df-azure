<?php

namespace DreamFactory\Core\Azure\Services;

use DreamFactory\Core\Azure\Components\DocumentDBConnection;
use DreamFactory\Core\Azure\Database\Schema\DocumentDbSchema;
use DreamFactory\Core\Azure\Resources\DocumentDbTable;
use DreamFactory\Core\Resources\DbSchemaResource;
use DreamFactory\Core\Services\BaseDbService;
use DreamFactory\Core\Utility\Session;

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

        $this->dbConn = new DocumentDBConnection($uri, $key, $database);
        /** @noinspection PhpParamsInspection */
        $this->schema = new DocumentDbSchema($this->dbConn);
        $this->schema->setCache($this);
        $this->schema->setExtraStore($this);
    }
}