<?php
namespace DreamFactory\Core\Azure\Services;

use DreamFactory\Core\Components\DbSchemaExtras;
use DreamFactory\Core\Database\TableSchema;
use DreamFactory\Core\Utility\Session;
use DreamFactory\Library\Utility\ArrayUtils;
use DreamFactory\Core\Exceptions\InternalServerErrorException;
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
    use DbSchemaExtras;

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
    protected $defaultPartitionKey = null;
    /**
     * @var array
     */
    protected $tableNames = [];
    /**
     * @var array
     */
    protected $tables = [];

    /**
     * @var array
     */
    protected static $resources = [
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
        Session::replaceLookups( $config, true );

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
        } catch (\Exception $ex) {
            throw new InternalServerErrorException("Windows Azure Table Service Exception:\n{$ex->getMessage()}");
        }
    }

    /**
     * Object destructor
     */
    public function __destruct()
    {
        try {
            $this->dbConn = null;
        } catch (\Exception $ex) {
            error_log("Failed to disconnect from database.\n{$ex->getMessage()}");
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

    /**
     */
    public function getDefaultPartitionKey()
    {
        return $this->defaultPartitionKey;
    }

    public function getTables()
    {
        /** @var QueryTablesResult $result */
        $result = $this->dbConn->queryTables();

        $out = $result->getTables();

        return $out;
    }

    public function getTableNames($schema = null, $refresh = false, $use_alias = false)
    {
        if ($refresh ||
            (empty($this->tableNames) &&
                (null === $this->tableNames = $this->getFromCache('table_names')))
        ) {
            /** @type TableSchema[] $names */
            $names = [];
            /** @var QueryTablesResult $result */
            $result = $this->dbConn->queryTables();
            $tables = $result->getTables();
            foreach ($tables as $table) {
                $names[strtolower($table)] = new TableSchema(['name' => $table]);
            }
            // merge db extras
            if (!empty($extrasEntries = $this->getSchemaExtrasForTables($tables, false))) {
                foreach ($extrasEntries as $extras) {
                    if (!empty($extraName = strtolower(strval($extras['table'])))) {
                        if (array_key_exists($extraName, $tables)) {
                            $names[$extraName]->fill($extras);
                        }
                    }
                }
            }
            $this->tableNames = $names;
            $this->addToCache('table_names', $this->tableNames, true);
        }

        return $this->tableNames;
    }

    public function refreshTableCache()
    {
        $this->removeFromCache('table_names');
        $this->tableNames = [];
        $this->tables = [];
    }
}