<?php
namespace DreamFactory\Core\Azure\Services;

use DreamFactory\Core\Azure\Database\Schema\AzureTableSchema;
use DreamFactory\Core\Azure\Resources\Table as TableResource;
use DreamFactory\Core\Exceptions\InternalServerErrorException;
use DreamFactory\Core\Resources\DbSchemaResource;
use DreamFactory\Core\Services\BaseDbService;
use DreamFactory\Core\Utility\Session;
use MicrosoftAzure\Storage\Common\ServicesBuilder;

/**
 * Table
 *
 * A service to handle AzureTables NoSQL (schema-less) database
 * services accessed through the REST API.
 */
class Table extends BaseDbService
{
    //*************************************************************************
    //	Constants
    //*************************************************************************

    /**
     * Define partitioning field
     */
    const PARTITION_KEY = 'PartitionKey';

    //*************************************************************************
    //	Members
    //*************************************************************************

    /**
     * @var string
     */
    protected $defaultPartitionKey = null;

    /**
     * @var array
     */
    protected static $resources = [
        DbSchemaResource::RESOURCE_NAME        => [
            'name'       => DbSchemaResource::RESOURCE_NAME,
            'class_name' => DbSchemaResource::class,
            'label'      => 'Schema',
        ],
        TableResource::RESOURCE_NAME => [
            'name'       => TableResource::RESOURCE_NAME,
            'class_name' => TableResource::class,
            'label'      => 'Table',
        ],
    ];

    //*************************************************************************
    //	Methods
    //*************************************************************************

    /**
     * Create a new AzureTablesSvc
     *
     * @param array $settings
     *
     * @throws \InvalidArgumentException
     * @throws \Exception
     */
    public function __construct($settings = array())
    {
        parent::__construct($settings);

        $config = (array)array_get($settings, 'config');
        Session::replaceLookups( $config, true );

        $dsn = strval(array_get($config, 'connection_string'));
        if (empty($dsn)) {
            $name = array_get($config, 'account_name', array_get($config, 'AccountName'));
            if (empty($name)) {
                throw new \InvalidArgumentException('WindowsAzure account name can not be empty.');
            }

            $key = array_get($config, 'account_key', array_get($config, 'AccountKey'));
            if (empty($key)) {
                throw new \InvalidArgumentException('WindowsAzure account key can not be empty.');
            }

            $protocol = array_get($config, 'protocol', 'https');
            $dsn = "DefaultEndpointsProtocol=$protocol;AccountName=$name;AccountKey=$key";
        }

        // set up a default partition key
        $partitionKey = array_get($config, static::PARTITION_KEY);
        if (!empty($partitionKey)) {
            $this->defaultPartitionKey = $partitionKey;
        }

        try {
            $this->dbConn = ServicesBuilder::getInstance()->createTableService($dsn);
            /** @noinspection PhpParamsInspection */
            $this->schema = new AzureTableSchema($this->dbConn);
            $this->schema->setCache($this);
            $this->schema->setExtraStore($this);
        } catch (\Exception $ex) {
            throw new InternalServerErrorException("Windows Azure Table Service Exception:\n{$ex->getMessage()}");
        }
    }

    /**
     */
    public function getDefaultPartitionKey()
    {
        return $this->defaultPartitionKey;
    }
}