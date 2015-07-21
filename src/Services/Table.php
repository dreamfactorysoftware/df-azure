<?php
namespace DreamFactory\Core\Azure\Services;

use DreamFactory\Library\Utility\ArrayUtils;
use DreamFactory\Core\Exceptions\BadRequestException;
use DreamFactory\Core\Exceptions\InternalServerErrorException;
use DreamFactory\Core\Exceptions\NotFoundException;
use DreamFactory\Core\Contracts\ServiceResponseInterface;
use DreamFactory\Core\Services\BaseNoSqlDbService;
use DreamFactory\Core\Azure\Resources\Schema;
use DreamFactory\Core\Azure\Resources\Table as TableResource;
use WindowsAzure\Common\ServicesBuilder;
use WindowsAzure\Table\Models\QueryTablesResult;
use WindowsAzure\Table\TableRestProxy;

/**
 * Table
 *
 * A service to handle AzureTables NoSQL (schema-less) database
 * services accessed through the REST API.
 */
class Table extends BaseNoSqlDbService
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
     * @var TableRestProxy|null
     */
    protected $dbConn = null;
    /**
     * @var string
     */
    protected $_defaultPartitionKey = null;

    /**
     * @var array
     */
    protected $resources = [
        Schema::RESOURCE_NAME        => [
            'name'       => Schema::RESOURCE_NAME,
            'class_name' => Schema::class,
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

        $config = ArrayUtils::clean(ArrayUtils::get($settings, 'config'));
//        Session::replaceLookups( $config, true );

        $dsn = strval(ArrayUtils::get($config, 'connection_string'));
        if (empty($dsn)) {
            $name = ArrayUtils::get($config, 'account_name', ArrayUtils::get($config, 'AccountName'));
            if (empty($name)) {
                throw new \InvalidArgumentException('WindowsAzure account name can not be empty.');
            }

            $key = ArrayUtils::get($config, 'account_key', ArrayUtils::get($config, 'AccountKey'));
            if (empty($key)) {
                throw new \InvalidArgumentException('WindowsAzure account key can not be empty.');
            }

            $protocol = ArrayUtils::get($config, 'protocol', 'https');
            $dsn = "DefaultEndpointsProtocol=$protocol;AccountName=$name;AccountKey=$key";
        }

        // set up a default partition key
        $partitionKey = ArrayUtils::get($config, static::PARTITION_KEY);
        if (!empty($partitionKey)) {
            $this->defaultPartitionKey = $partitionKey;
        }

        try {
            $this->dbConn = ServicesBuilder::getInstance()->createTableService($dsn);
        } catch (\Exception $_ex) {
            throw new InternalServerErrorException("Windows Azure Table Service Exception:\n{$_ex->getMessage()}");
        }
    }

    /**
     * Object destructor
     */
    public function __destruct()
    {
        try {
            $this->dbConn = null;
        } catch (\Exception $_ex) {
            error_log("Failed to disconnect from database.\n{$_ex->getMessage()}");
        }
    }

    /**
     * @throws \Exception
     */
    public function getConnection()
    {
        if (!isset($this->dbConn)) {
            throw new InternalServerErrorException('Database connection has not been initialized.');
        }

        return $this->dbConn;
    }

    public function getTables()
    {
        /** @var QueryTablesResult $_result */
        $_result = $this->dbConn->queryTables();

        $out = $_result->getTables();

        return $out;
    }

    /**
     * @param string $name
     *
     * @return string
     * @throws BadRequestException
     * @throws NotFoundException
     */
    public function correctTableName(&$name)
    {
        static $_existing = null;

        if (!$_existing) {
            $_existing = $this->getTables();
        }

        if (empty($name)) {
            throw new BadRequestException('Table name can not be empty.');
        }

        if (false === array_search($name, $_existing)) {
            throw new NotFoundException("Table '$name' not found.");
        }

        return $name;
    }

    /**
     * {@InheritDoc}
     */
    protected function handleResource(array $resources)
    {
        try {
            return parent::handleResource($resources);
        } catch (NotFoundException $_ex) {
            // If version 1.x, the resource could be a table
//            if ($this->request->getApiVersion())
//            {
//                $resource = $this->instantiateResource( Table::class, [ 'name' => $this->resource ] );
//                $newPath = $this->resourceArray;
//                array_shift( $newPath );
//                $newPath = implode( '/', $newPath );
//
//                return $resource->handleRequest( $this->request, $newPath, $this->outputFormat );
//            }

            throw $_ex;
        }
    }

    /**
     * {@inheritdoc}
     */
    public function getAccessList()
    {
        $_resources = [];

//        $refresh = $this->request->getParameterAsBool( 'refresh' );

        $_name = Schema::RESOURCE_NAME . '/';
        $_access = $this->getPermissions($_name);
        if (!empty($_access)) {
            $_resources[] = $_name;
            $_resources[] = $_name . '*';
        }

        $_result = $this->getTables();
        foreach ($_result as $_name) {
            $_name = Schema::RESOURCE_NAME . '/' . $_name;
            $_access = $this->getPermissions($_name);
            if (!empty($_access)) {
                $_resources[] = $_name;
            }
        }

        $_name = TableResource::RESOURCE_NAME . '/';
        $_access = $this->getPermissions($_name);
        if (!empty($_access)) {
            $_resources[] = $_name;
            $_resources[] = $_name . '*';
        }

        foreach ($_result as $_name) {
            $_name = TableResource::RESOURCE_NAME . '/' . $_name;
            $_access = $this->getPermissions($_name);
            if (!empty($_access)) {
                $_resources[] = $_name;
            }
        }

        return $_resources;
    }
}