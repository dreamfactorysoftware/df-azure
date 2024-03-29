<?php
namespace DreamFactory\Core\Azure\Services;

use DreamFactory\Core\Azure\Database\Schema\AzureTableSchema;
use DreamFactory\Core\Azure\Resources\Table as TableResource;
use DreamFactory\Core\Database\Resources\BaseDbTableResource;
use DreamFactory\Core\Exceptions\InternalServerErrorException;
use DreamFactory\Core\Database\Services\BaseDbService;
use MicrosoftAzure\Storage\Table\TableRestProxy;
use Illuminate\Support\Arr;

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
     * @var string
     */
    protected $dsn = null;

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

        $this->dsn = strval(Arr::get($this->config, 'connection_string'));
        if (empty($this->dsn)) {
            $name = Arr::get($this->config, 'account_name', Arr::get($this->config, 'AccountName'));
            if (empty($name)) {
                throw new \InvalidArgumentException('WindowsAzure account name can not be empty.');
            }

            $key = Arr::get($this->config, 'account_key', Arr::get($this->config, 'AccountKey'));
            if (empty($key)) {
                throw new \InvalidArgumentException('WindowsAzure account key can not be empty.');
            }

            $protocol = Arr::get($this->config, 'protocol', 'https');
            $this->dsn = "DefaultEndpointsProtocol=$protocol;AccountName=$name;AccountKey=$key;TableEndpoint=https://$name.table.cosmos.azure.com:443/";
        }

        // set up a default partition key
        $partitionKey = Arr::get($this->config, static::PARTITION_KEY);
        if (!empty($partitionKey)) {
            $this->defaultPartitionKey = $partitionKey;
        }

        $this->setConfigBasedCachePrefix(Arr::get($this->config, 'account_name') . ':');
    }

    public function getResourceHandlers()
    {
        $handlers = parent::getResourceHandlers();

        $handlers[TableResource::RESOURCE_NAME] = [
            'name'       => TableResource::RESOURCE_NAME,
            'class_name' => TableResource::class,
            'label'      => 'Table',
        ];

        return $handlers;
    }

    protected function initializeConnection()
    {
        try {
            $this->dbConn = TableRestProxy::createTableService($this->dsn);
            /** @noinspection PhpParamsInspection */
            $this->schema = new AzureTableSchema($this->dbConn);
        } catch (\Exception $ex) {
            throw new InternalServerErrorException("Windows Azure Table Service Exception:\n{$ex->getMessage()}");
        }
    }

    public function getDefaultPartitionKey()
    {
        return $this->defaultPartitionKey;
    }
}