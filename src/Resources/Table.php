<?php
namespace DreamFactory\Core\Azure\Resources;

use DreamFactory\Library\Utility\ArrayUtils;
use DreamFactory\Library\Utility\Enums\Verbs;
use DreamFactory\Library\Utility\Inflector;
use DreamFactory\Core\Exceptions\BadRequestException;
use DreamFactory\Core\Exceptions\InternalServerErrorException;
use DreamFactory\Core\Exceptions\NotFoundException;
use DreamFactory\Core\Exceptions\RestException;
use DreamFactory\Core\Resources\BaseDbTableResource;
use DreamFactory\Core\Utility\DbUtilities;
use DreamFactory\Core\Azure\Services\Table as TableService;
use WindowsAzure\Common\ServiceException;
use WindowsAzure\Table\Models\BatchError;
use WindowsAzure\Table\Models\BatchOperations;
use WindowsAzure\Table\Models\BatchResult;
use WindowsAzure\Table\Models\EdmType;
use WindowsAzure\Table\Models\Entity;
use WindowsAzure\Table\Models\Filters\QueryStringFilter;
use WindowsAzure\Table\Models\GetEntityResult;
use WindowsAzure\Table\Models\InsertEntityResult;
use WindowsAzure\Table\Models\Property;
use WindowsAzure\Table\Models\QueryEntitiesOptions;
use WindowsAzure\Table\Models\QueryEntitiesResult;
use WindowsAzure\Table\Models\UpdateEntityResult;

class Table extends BaseDbTableResource
{
    //*************************************************************************
    //	Constants
    //*************************************************************************

    /**
     * Default record identifier field
     */
    const DEFAULT_ID_FIELD = 'RowKey';
    /**
     * Define identifying field
     */
    const ROW_KEY = 'RowKey';
    /**
     * Define partitioning field
     */
    const PARTITION_KEY = 'PartitionKey';

    //*************************************************************************
    //	Members
    //*************************************************************************

    /**
     * @var null|TableService
     */
    protected $service = null;
    /**
     * @var string
     */
    protected $_defaultPartitionKey = null;
    /**
     * @var null | BatchOperations
     */
    protected $batchOps = null;
    /**
     * @var null | BatchOperations
     */
    protected $backupOps = null;

    //*************************************************************************
    //	Methods
    //*************************************************************************

    /**
     * @return null|TableService
     */
    public function getService()
    {
        return $this->service;
    }

    /**
     * {@inheritdoc}
     */
    public function listResources($fields = null)
    {
//        $refresh = $this->request->queryBool('refresh');

        $_names = $this->service->getTables();

        if (empty($fields)) {
            return ['resource' => $_names];
        }

        $_extras =
            DbUtilities::getSchemaExtrasForTables($this->service->getServiceId(), $_names, false, 'table,label,plural');

        $_tables = [];
        foreach ($_names as $name) {
            $label = '';
            $plural = '';
            foreach ($_extras as $each) {
                if (0 == strcasecmp($name, ArrayUtils::get($each, 'table', ''))) {
                    $label = ArrayUtils::get($each, 'label');
                    $plural = ArrayUtils::get($each, 'plural');
                    break;
                }
            }

            if (empty($label)) {
                $label = Inflector::camelize($name, ['_', '.'], true);
            }

            if (empty($plural)) {
                $plural = Inflector::pluralize($label);
            }

            $_tables[] = ['name' => $name, 'label' => $label, 'plural' => $plural];
        }

        return $this->makeResourceList($_tables, 'name', $fields, 'resource');
    }

    /**
     * {@inheritdoc}
     */
    public function updateRecordsByFilter($table, $record, $filter = null, $params = array(), $extras = array())
    {
        $record = DbUtilities::validateAsArray($record, null, false, 'There are no fields in the record.');

        $_fields = ArrayUtils::get($extras, 'fields');
        $_ssFilters = ArrayUtils::get($extras, 'ss_filters');
        try {
            // parse filter
            $filter = static::buildCriteriaArray($filter, $params, $_ssFilters);
            /** @var Entity[] $_entities */
            $_entities = $this->queryEntities($table, $filter, $_fields, $extras);
            foreach ($_entities as $_entity) {
                $_entity = static::parseRecordToEntity($record, $_entity);
                $this->service->getConnection()->updateEntity($table, $_entity);
            }

            $_out = static::parseEntitiesToRecords($_entities, $_fields);

            return $_out;
        } catch (\Exception $_ex) {
            throw new InternalServerErrorException("Failed to update records in '$table'.\n{$_ex->getMessage()}");
        }
    }

    /**
     * {@inheritdoc}
     */
    public function patchRecordsByFilter($table, $record, $filter = null, $params = array(), $extras = array())
    {
        $record = DbUtilities::validateAsArray($record, null, false, 'There are no fields in the record.');

        $_fields = ArrayUtils::get($extras, 'fields');
        $_ssFilters = ArrayUtils::get($extras, 'ss_filters');
        try {
            // parse filter
            $filter = static::buildCriteriaArray($filter, $params, $_ssFilters);
            /** @var Entity[] $_entities */
            $_entities = $this->queryEntities($table, $filter, $_fields, $extras);
            foreach ($_entities as $_entity) {
                $_entity = static::parseRecordToEntity($record, $_entity);
                $this->service->getConnection()->mergeEntity($table, $_entity);
            }

            $_out = static::parseEntitiesToRecords($_entities, $_fields);

            return $_out;
        } catch (\Exception $_ex) {
            throw new InternalServerErrorException("Failed to patch records in '$table'.\n{$_ex->getMessage()}");
        }
    }

    /**
     * {@inheritdoc}
     */
    public function truncateTable($table, $extras = array())
    {
        // todo Better way?
        parent::truncateTable($table, $extras);
    }

    /**
     * {@inheritdoc}
     */
    public function deleteRecordsByFilter($table, $filter, $params = array(), $extras = array())
    {
        if (empty($filter)) {
            throw new BadRequestException("Filter for delete request can not be empty.");
        }

        $_fields = ArrayUtils::get($extras, 'fields');
        $_ssFilters = ArrayUtils::get($extras, 'ss_filters');
        try {
            $filter = static::buildCriteriaArray($filter, $params, $_ssFilters);
            /** @var Entity[] $_entities */
            $_entities = $this->queryEntities($table, $filter, $_fields, $extras);
            foreach ($_entities as $_entity) {
                $_partitionKey = $_entity->getPartitionKey();
                $_rowKey = $_entity->getRowKey();
                $this->service->getConnection()->deleteEntity($table, $_partitionKey, $_rowKey);
            }

            $_out = static::parseEntitiesToRecords($_entities, $_fields);

            return $_out;
        } catch (\Exception $_ex) {
            throw new InternalServerErrorException("Failed to delete records from '$table'.\n{$_ex->getMessage()}");
        }
    }

    /**
     * {@inheritdoc}
     */
    public function retrieveRecordsByFilter($table, $filter = null, $params = array(), $extras = array())
    {
        $_fields = ArrayUtils::get($extras, 'fields');
        $_ssFilters = ArrayUtils::get($extras, 'ss_filters');

        $_options = new QueryEntitiesOptions();
        $_options->setSelectFields(array());
        if (!empty($_fields) && ('*' != $_fields)) {
            $_fields = array_map('trim', explode(',', trim($_fields, ',')));
            $_options->setSelectFields($_fields);
        }

        $limit = intval(ArrayUtils::get($extras, 'limit', 0));
        if ($limit > 0) {
            $_options->setTop($limit);
        }

        $filter = static::buildCriteriaArray($filter, $params, $_ssFilters);
        $_out = $this->queryEntities($table, $filter, $_fields, $extras, true);

        return $_out;
    }

    /**
     * @param      $table
     * @param null $fields_info
     * @param null $requested_fields
     * @param null $requested_types
     *
     * @return array
     */
    protected function getIdsInfo($table, $fields_info = null, &$requested_fields = null, $requested_types = null)
    {
        $requested_fields = array(static::PARTITION_KEY, static::ROW_KEY); // can only be this
        $_ids = array(
            array('name' => static::PARTITION_KEY, 'type' => 'string', 'required' => true),
            array('name' => static::ROW_KEY, 'type' => 'string', 'required' => true)
        );

        return $_ids;
    }

    /**
     * @param        $table
     * @param string $parsed_filter
     * @param string $fields
     * @param array  $extras
     * @param bool   $parse_results
     *
     * @throws \Exception
     * @return array
     */
    protected function queryEntities(
        $table,
        $parsed_filter = '',
        $fields = null,
        $extras = array(),
        $parse_results = false
    ){
        $_options = new QueryEntitiesOptions();
        $_options->setSelectFields(array());

        if (!empty($fields) && ('*' != $fields)) {
            if (!is_array($fields)) {
                $fields = array_map('trim', explode(',', trim($fields, ',')));
            }
            $_options->setSelectFields($fields);
        }

        $limit = intval(ArrayUtils::get($extras, 'limit', 0));
        if ($limit > 0) {
            $_options->setTop($limit);
        }

        if (!empty($parsed_filter)) {
            $_query = new QueryStringFilter($parsed_filter);
            $_options->setFilter($_query);
        }

        try {
            /** @var QueryEntitiesResult $_result */
            $_result = $this->service->getConnection()->queryEntities($table, $_options);

            /** @var Entity[] $entities */
            $_entities = $_result->getEntities();

            if ($parse_results) {
                return static::parseEntitiesToRecords($_entities);
            }

            return $_entities;
        } catch (ServiceException $_ex) {
            throw new InternalServerErrorException("Failed to filter items from '$table' on Windows Azure Tables service.\n" .
                $_ex->getMessage());
        }
    }

    /**
     * @param array $record
     * @param array $fields_info
     * @param array $filter_info
     * @param bool  $for_update
     * @param array $old_record
     *
     * @return array
     * @throws \Exception
     */
    protected function parseRecord($record, $fields_info, $filter_info = null, $for_update = false, $old_record = null)
    {
        $_parsed = (empty($fields_info)) ? $record : array();

        unset($_parsed['Timestamp']); // not set-able

        if (!empty($fields_info)) {
            $_keys = array_keys($record);
            $_values = array_values($record);
            foreach ($fields_info as $_fieldInfo) {
                $_name = ArrayUtils::get($_fieldInfo, 'name', '');
                $_type = ArrayUtils::get($_fieldInfo, 'type');
                $_pos = array_search($_name, $_keys);
                if (false !== $_pos) {
                    $_fieldVal = ArrayUtils::get($_values, $_pos);
                    // due to conversion from XML to array, null or empty xml elements have the array value of an empty array
                    if (is_array($_fieldVal) && empty($_fieldVal)) {
                        $_fieldVal = null;
                    }

                    /** validations **/

                    $_validations = ArrayUtils::get($_fieldInfo, 'validation');

                    if (!static::validateFieldValue($_name, $_fieldVal, $_validations, $for_update, $_fieldInfo)) {
                        unset($_keys[$_pos]);
                        unset($_values[$_pos]);
                        continue;
                    }

                    $_parsed[$_name] = $_fieldVal;
                    unset($_keys[$_pos]);
                    unset($_values[$_pos]);
                }

                // add or override for specific fields
                switch ($_type) {
                    case 'timestamp_on_create':
                        if (!$for_update) {
                            $_parsed[$_name] = new \MongoDate();
                        }
                        break;
                    case 'timestamp_on_update':
                        $_parsed[$_name] = new \MongoDate();
                        break;
                    case 'user_id_on_create':
                        if (!$for_update) {
                            $userId = 1;//Session::getCurrentUserId();
                            if (isset($userId)) {
                                $_parsed[$_name] = $userId;
                            }
                        }
                        break;
                    case 'user_id_on_update':
                        $userId = 1;//Session::getCurrentUserId();
                        if (isset($userId)) {
                            $_parsed[$_name] = $userId;
                        }
                        break;
                }
            }
        }

        if (!empty($filter_info)) {
            $this->validateRecord($_parsed, $filter_info, $for_update, $old_record);
        }

        return $_parsed;
    }

    /**
     * @param array       $record
     * @param null|Entity $entity
     * @param array       $exclude List of keys to exclude from adding to Entity
     *
     * @return Entity
     */
    protected static function parseRecordToEntity($record = array(), $entity = null, $exclude = array())
    {
        if (empty($entity)) {
            $entity = new Entity();
        }
        foreach ($record as $_key => $_value) {
            if (false === array_search($_key, $exclude)) {
                // valid types
//				const DATETIME = 'Edm.DateTime';
//				const BINARY   = 'Edm.Binary';
//				const GUID     = 'Edm.Guid';
                $_edmType = EdmType::STRING;
                switch (gettype($_value)) {
                    case 'boolean':
                        $_edmType = EdmType::BOOLEAN;
                        break;
                    case 'double':
                    case 'float':
                        $_edmType = EdmType::DOUBLE;
                        break;
                    case 'integer':
                        $_edmType = ($_value > 2147483647) ? EdmType::INT64 : EdmType::INT32;
                        break;
                }
                if ($entity->getProperty($_key)) {
                    $_prop = new Property();
                    $_prop->setEdmType($_edmType);
                    $_prop->setValue($_value);
                    $entity->setProperty($_key, $_prop);
                } else {
                    $entity->addProperty($_key, $_edmType, $_value);
                }
            }
        }

        return $entity;
    }

    /**
     * @param null|Entity  $entity
     * @param string|array $include List of keys to include in the output record
     * @param array        $record
     *
     * @return array
     */
    protected static function parseEntityToRecord($entity, $include = '*', $record = array())
    {
        if (!empty($entity)) {
            if (empty($include)) {
                $record[static::PARTITION_KEY] = $entity->getPartitionKey();
                $record[static::ROW_KEY] = $entity->getRowKey();
            } elseif ('*' == $include) {
                // return all properties
                /** @var Property[] $properties */
                $properties = $entity->getProperties();
                foreach ($properties as $key => $property) {
                    $record[$key] = $property->getValue();
                }
            } else {
                if (!is_array($include)) {
                    $include = array_map('trim', explode(',', trim($include, ',')));
                }
                foreach ($include as $key) {
                    $record[$key] = $entity->getPropertyValue($key);
                }
            }
        }

        return $record;
    }

    protected static function parseEntitiesToRecords($entities, $include = '*', $records = array())
    {
        if (!is_array($records)) {
            $records = array();
        }
        foreach ($entities as $_entity) {
            if ($_entity instanceof BatchError) {
                /** @var ServiceException $_error */
                $_error = $_entity->getError();
                throw $_error;
            }
            if ($_entity instanceof InsertEntityResult) {
                /** @var InsertEntityResult $_entity */
                $_entity = $_entity->getEntity();
                $records[] = static::parseEntityToRecord($_entity, $include);
            } else {
                $records[] = static::parseEntityToRecord($_entity, $include);
            }
        }

        return $records;
    }

    protected static function buildCriteriaArray($filter, $params = null, $ss_filters = null)
    {
        // interpret any parameter values as lookups
        $params = static::interpretRecordValues($params);

        // build filter array if necessary, add server-side filters if necessary
        if (!is_array($filter)) {
//            Session::replaceLookups( $filter );
            $_criteria = static::parseFilter($filter, $params);
        } else {
            $_criteria = $filter;
        }
        $_serverCriteria = static::buildSSFilterArray($ss_filters);
        if (!empty($_serverCriteria)) {
            $_criteria = (empty($_criteria)) ? $_serverCriteria : "( $_serverCriteria ) and ( $_criteria )";
        }

        return $_criteria;
    }

    protected static function buildSSFilterArray($ss_filters)
    {
        if (empty($ss_filters)) {
            return '';
        }

        // build the server side criteria
        $_filters = ArrayUtils::get($ss_filters, 'filters');
        if (empty($_filters)) {
            return '';
        }

        $_combiner = ArrayUtils::get($ss_filters, 'filter_op', 'and');
        switch (strtoupper($_combiner)) {
            case 'AND':
            case 'OR':
                break;
            default:
                // log and bail
                throw new InternalServerErrorException('Invalid server-side filter configuration detected.');
        }

        $_criteria = '';
        foreach ($_filters as $_filter) {
            $_name = ArrayUtils::get($_filter, 'name');
            $_op = ArrayUtils::get($_filter, 'operator');
            if (empty($_name) || empty($_op)) {
                // log and bail
                throw new InternalServerErrorException('Invalid server-side filter configuration detected.');
            }

            $_value = ArrayUtils::get($_filter, 'value');
            $_value = static::interpretFilterValue($_value);

            $_temp = static::parseFilter("$_name $_op $_value");
            if (!empty($_criteria)) {
                $_criteria .= " $_combiner ";
            }
            $_criteria .= $_temp;
        }

        return $_criteria;
    }

    /**
     * @param string|array $filter Filter for querying records by
     * @param array        $params replacement parameters
     *
     * @return array
     */
    protected static function parseFilter($filter, $params = array())
    {
        if (empty($filter)) {
            return '';
        }

        if (is_array($filter)) {
            return ''; // todo need to build from array of parts
        }

        // handle logical operators first
        // supported logical operators are or, and, not
        $_search = array(' || ', ' && ', ' OR ', ' AND ', ' NOR ', ' NOT ');
        $_replace = array(' or ', ' and ', ' or ', ' and ', ' nor ', ' not ');
        $filter = trim(str_ireplace($_search, $_replace, ' ' . $filter)); // space added for 'not' case

        // the rest should be comparison operators
        // supported comparison operators are eq, ne, gt, ge, lt, le
        $_search =
            array('!=', '>=', '<=', '>', '<', '=', ' EQ ', ' NE ', ' LT ', ' LTE ', ' LE ', ' GT ', ' GTE', ' GE ');
        $_replace = array(
            ' ne ',
            ' ge ',
            ' le ',
            ' gt ',
            ' lt ',
            ' eq ',
            ' eq ',
            ' ne ',
            ' lt ',
            ' le ',
            ' le ',
            ' gt ',
            ' ge ',
            ' ge '
        );
        $filter = trim(str_ireplace($_search, $_replace, $filter));

//			WHERE name LIKE "%Joe%"	not supported
//			WHERE name LIKE "%Joe"	not supported
//			WHERE name LIKE "Joe%"	name ge 'Joe' and name lt 'Jof';
//			if ( ( '%' == $_val[ strlen( $_val ) - 1 ] ) &&
//				 ( '%' != $_val[0] ) )
//			{
//			}

        if (!empty($params)) {
            foreach ($params as $_name => $_value) {
                $filter = str_replace($_name, $_value, $filter);
            }
        }

        return $filter;
    }

    protected static function buildIdsFilter($ids, $partition_key = null)
    {
        if (empty($ids)) {
            return null;
        }

        if (!is_array($ids)) {
            $ids = array_map('trim', explode(',', trim($ids, ',')));
        }

        $_filters = array();
        $_filter = '';
        $_count = 0;
        foreach ($ids as $_id) {
            if (!empty($_filter)) {
                $_filter .= ' or ';
            }
            $_filter .= static::ROW_KEY . " eq '$_id'";
            $_count++;
            if ($_count >= 14) // max comparisons is 15, leave one for partition key
            {
                if (!empty($partition_key)) {
                    $_filter = static::PARTITION_KEY . " eq '$partition_key' and ( $_filter )";
                }
                $_filters[] = $_filter;
                $_count = 0;
            }
        }

        if (!empty($_filter)) {
            if (!empty($partition_key)) {
                $_filter = static::PARTITION_KEY . " eq '$partition_key' and ( $_filter )";
            }
            $_filters[] = $_filter;
        }

        return $_filters;
    }

    protected function checkForIds(&$record, $ids_info, $extras = null, $on_create = false, $remove = false)
    {
        $_id = null;
        if (!empty($ids_info)) {
            if (1 == count($ids_info)) {
                $_info = $ids_info[0];
                $_name = ArrayUtils::get($_info, 'name');
                $_value = (is_array($record)) ? ArrayUtils::get($record, $_name, null, $remove) : $record;
                if (!empty($_value)) {
                    $_type = ArrayUtils::get($_info, 'type');
                    switch ($_type) {
                        case 'int':
                            $_value = intval($_value);
                            break;
                        case 'string':
                            $_value = strval($_value);
                            break;
                    }
                    $_id = $_value;
                } else {
                    $_required = ArrayUtils::getBool($_info, 'required');
                    // could be passed in as a parameter affecting all records
                    $_param = ArrayUtils::get($extras, $_name);
                    if ($on_create && $_required && empty($_param)) {
                        return false;
                    }
                }
            } else {
                $_id = array();
                foreach ($ids_info as $_info) {
                    $_name = ArrayUtils::get($_info, 'name');
                    $_value = ArrayUtils::get($record, $_name, null, $remove);
                    if (!empty($_value)) {
                        $_type = ArrayUtils::get($_info, 'type');
                        switch ($_type) {
                            case 'int':
                                $_value = intval($_value);
                                break;
                            case 'string':
                                $_value = strval($_value);
                                break;
                        }
                        $_id[$_name] = $_value;
                    } else {
                        $_required = ArrayUtils::getBool($_info, 'required');
                        // could be passed in as a parameter affecting all records
                        $_param = ArrayUtils::get($extras, $_name);
                        if ($on_create && $_required && empty($_param)) {
                            if (!is_array($record) && (static::ROW_KEY == $_name)) {
                                $_id[$_name] = $record;
                            } else {
                                return false;
                            }
                        }
                    }
                }
            }
        }

        if (!empty($_id)) {
            return $_id;
        } elseif ($on_create) {
            return array();
        }

        return false;
    }

    /**
     * {@inheritdoc}
     */
    protected function initTransaction($handle = null)
    {
        $this->batchOps = null;
        $this->backupOps = null;

        return parent::initTransaction($handle);
    }

    /**
     * {@inheritdoc}
     */
    protected function addToTransaction(
        $record = null,
        $id = null,
        $extras = null,
        $rollback = false,
        $continue = false,
        $single = false
    ){
        $_ssFilters = ArrayUtils::get($extras, 'ss_filters');
        $_fields = ArrayUtils::get($extras, 'fields');
        $_fieldsInfo = ArrayUtils::get($extras, 'fields_info');
        $_requireMore = ArrayUtils::get($extras, 'require_more');
        $_updates = ArrayUtils::get($extras, 'updates');
        $_partitionKey = ArrayUtils::get($extras, static::PARTITION_KEY);

        if (!is_array($id)) {
            $id = array(static::ROW_KEY => $id, static::PARTITION_KEY => $_partitionKey);
        }
        if (!empty($_partitionKey)) {
            $id[static::PARTITION_KEY] = $_partitionKey;
        }

        if (!empty($_updates)) {
            foreach ($id as $_field => $_value) {
                if (!isset($_updates[$_field])) {
                    $_updates[$_field] = $_value;
                }
            }
            $record = $_updates;
        } elseif (!empty($record)) {
            if (!empty($_partitionKey)) {
                $record[static::PARTITION_KEY] = $_partitionKey;
            }
        }

        if (!empty($record)) {
            $_forUpdate = false;
            switch ($this->getAction()) {
                case Verbs::PUT:
                case Verbs::MERGE:
                case Verbs::PATCH:
                    $_forUpdate = true;
                    break;
            }

            $record = $this->parseRecord($record, $_fieldsInfo, $_ssFilters, $_forUpdate);
            if (empty($record)) {
                throw new BadRequestException('No valid fields were found in record.');
            }

            $_entity = static::parseRecordToEntity($record);
        } else {
            $_entity = static::parseRecordToEntity($id);
        }

        $_partKey = $_entity->getPartitionKey();
        if (empty($_partKey)) {
            throw new BadRequestException('No valid partition key found in request.');
        }

        $_rowKey = $_entity->getRowKey();
        if (empty($_rowKey)) {
            throw new BadRequestException('No valid row key found in request.');
        }

        // only allow batch if rollback and same partition
        $_batch = ($rollback && !empty($_partitionKey));
        $_out = array();
        switch ($this->getAction()) {
            case Verbs::POST:
                if ($_batch) {
                    if (!isset($this->batchOps)) {
                        $this->batchOps = new BatchOperations();
                    }
                    $this->batchOps->addInsertEntity($this->_transactionTable, $_entity);

                    // track record for output
                    return parent::addToTransaction($record);
                }

                /** @var InsertEntityResult $_result */
                $_result = $this->service->getConnection()->insertEntity($this->_transactionTable, $_entity);

                if ($rollback) {
                    $this->addToRollback($_entity);
                }

                $_out = static::parseEntityToRecord($_result->getEntity(), $_fields);
                break;
            case Verbs::PUT:
                if ($_batch) {
                    if (!isset($this->batchOps)) {
                        $this->batchOps = new BatchOperations();
                    }
                    $this->batchOps->addUpdateEntity($this->_transactionTable, $_entity);

                    // track record for output
                    return parent::addToTransaction($record);
                }

                if ($rollback) {
                    $_old = $this->service->getConnection()->getEntity(
                        $this->_transactionTable,
                        $_entity->getRowKey(),
                        $_entity->getPartitionKey()
                    );
                    $this->addToRollback($_old);
                }

                /** @var UpdateEntityResult $_result */
                $this->service->getConnection()->updateEntity($this->_transactionTable, $_entity);

                $_out = static::parseEntityToRecord($_entity, $_fields);
                break;
            case Verbs::MERGE:
            case Verbs::PATCH:
                if ($_batch) {
                    if (!isset($this->batchOps)) {
                        $this->batchOps = new BatchOperations();
                    }
                    $this->batchOps->addMergeEntity($this->_transactionTable, $_entity);

                    // track id for output
                    return parent::addToTransaction(null, $_rowKey);
                }

                if ($rollback || $_requireMore) {
                    $_old = $this->service->getConnection()->getEntity($this->_transactionTable, $_rowKey, $_partKey);
                    if ($rollback) {
                        $this->addToRollback($_old);
                    }
                    if ($_requireMore) {
                        $_out = array_merge(
                            static::parseEntityToRecord($_old, $_fields),
                            static::parseEntityToRecord($_entity, $_fields)
                        );
                    }
                }

                $_out = (empty($_out)) ? static::parseEntityToRecord($_entity, $_fields) : $_out;

                /** @var UpdateEntityResult $_result */
                $this->service->getConnection()->mergeEntity($this->_transactionTable, $_entity);
                break;
            case Verbs::DELETE:
                if ($_batch) {
                    if (!isset($this->batchOps)) {
                        $this->batchOps = new BatchOperations();
                    }
                    $this->batchOps->addDeleteEntity($this->_transactionTable, $_partKey, $_rowKey);

                    // track id for output
                    return parent::addToTransaction(null, $_rowKey);
                }

                if ($rollback || $_requireMore) {
                    $_old = $this->service->getConnection()->getEntity($this->_transactionTable, $_partKey, $_rowKey);
                    if ($rollback) {
                        $this->addToRollback($_old);
                    }
                    if ($_requireMore) {
                        $_out = static::parseEntityToRecord($_old, $_fields);
                    }
                }

                $this->service->getConnection()->deleteEntity($this->_transactionTable, $_partKey, $_rowKey);

                $_out = (empty($_out)) ? static::parseEntityToRecord($_entity, $_fields) : $_out;
                break;
            case Verbs::GET:
                if (!empty($_partitionKey)) {
                    // track id for output
                    return parent::addToTransaction(null, $_rowKey);
                }

                /** @var GetEntityResult $_result */
                $_result = $this->service->getConnection()->getEntity($this->_transactionTable, $_partKey, $_rowKey);

                $_out = static::parseEntityToRecord($_result->getEntity(), $_fields);
                break;
        }

        return $_out;
    }

    /**
     * {@inheritdoc}
     */
    protected function commitTransaction($extras = null)
    {
        if (!isset($this->batchOps) && empty($this->_batchIds) && empty($this->_batchRecords)) {
            return null;
        }

        $_fields = ArrayUtils::get($extras, 'fields');
        $_partitionKey = ArrayUtils::get($extras, static::PARTITION_KEY);

        $_out = array();
        switch ($this->getAction()) {
            case Verbs::POST:
            case Verbs::PUT:
                if (isset($this->batchOps)) {
                    /** @var BatchResult $_result */
                    $this->service->getConnection()->batch($this->batchOps);
                }
                if (!empty($this->_batchRecords)) {
                    $_out = static::parseEntitiesToRecords($this->_batchRecords, $_fields);
                }
                break;

            case Verbs::MERGE:
            case Verbs::PATCH:
                if (isset($this->batchOps)) {
                    /** @var BatchResult $_result */
                    $this->service->getConnection()->batch($this->batchOps);
                }
                if (!empty($this->_batchIds)) {
                    $_filters = static::buildIdsFilter($this->_batchIds, $_partitionKey);
                    foreach ($_filters as $_filter) {
                        $_temp = $this->queryEntities($this->_transactionTable, $_filter, $_fields, $extras, true);
                        $_out = array_merge($_out, $_temp);
                    }
                }
                break;

            case Verbs::DELETE:
                if (!empty($this->_batchIds)) {
                    $_filters = static::buildIdsFilter($this->_batchIds, $_partitionKey);
                    foreach ($_filters as $_filter) {
                        $_temp = $this->queryEntities($this->_transactionTable, $_filter, $_fields, $extras, true);
                        $_out = array_merge($_out, $_temp);
                    }
                }
                if (isset($this->batchOps)) {
                    /** @var BatchResult $_result */
                    $this->service->getConnection()->batch($this->batchOps);
                }
                break;

            case Verbs::GET:
                if (!empty($this->_batchIds)) {
                    $_filters = static::buildIdsFilter($this->_batchIds, $_partitionKey);
                    foreach ($_filters as $_filter) {
                        $_temp = $this->queryEntities($this->_transactionTable, $_filter, $_fields, $extras, true);
                        $_out = array_merge($_out, $_temp);
                    }
                }
                break;

            default:
                break;
        }

        $this->_batchIds = array();
        $this->_batchRecords = array();

        return $_out;
    }

    /**
     * {@inheritdoc}
     */
    protected function addToRollback($record)
    {
        if (!isset($this->backupOps)) {
            $this->backupOps = new BatchOperations();
        }
        switch ($this->getAction()) {
            case Verbs::POST:
                $this->backupOps->addDeleteEntity(
                    $this->_transactionTable,
                    $record->getPartitionKey(),
                    $record->getRowKey()
                );
                break;

            case Verbs::PUT:
            case Verbs::MERGE:
            case Verbs::PATCH:
            case Verbs::DELETE:
                $this->batchOps->addUpdateEntity($this->_transactionTable, $record);
                break;

            default:
                break;
        }

        return true;
    }

    /**
     * {@inheritdoc}
     */
    protected function rollbackTransaction()
    {
        if (!isset($this->backupOps)) {
            switch ($this->getAction()) {
                case Verbs::POST:
                case Verbs::PUT:
                case Verbs::PATCH:
                case Verbs::MERGE:
                case Verbs::DELETE:
                    /** @var BatchResult $_result */
                    $this->service->getConnection()->batch($this->backupOps);
                    break;

                default:
                    break;
            }

            $this->backupOps = null;
        }

        return true;
    }
}