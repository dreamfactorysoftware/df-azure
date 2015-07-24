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
    protected $defaultPartitionKey = null;
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
    public function getResources($only_handlers = false)
    {
        if ($only_handlers) {
            return [];
        }
//        $refresh = $this->request->queryBool('refresh');

        $names = $this->service->getTables();

        $extras =
            DbUtilities::getSchemaExtrasForTables($this->service->getServiceId(), $names, false, 'table,label,plural');

        $tables = [];
        foreach ($names as $name) {
            $label = '';
            $plural = '';
            foreach ($extras as $each) {
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

            $tables[] = ['name' => $name, 'label' => $label, 'plural' => $plural];
        }

        return $tables;
    }

    /**
     * {@inheritdoc}
     */
    public function updateRecordsByFilter($table, $record, $filter = null, $params = array(), $extras = array())
    {
        $record = DbUtilities::validateAsArray($record, null, false, 'There are no fields in the record.');

        $fields = ArrayUtils::get($extras, 'fields');
        $ssFilters = ArrayUtils::get($extras, 'ss_filters');
        try {
            // parse filter
            $filter = static::buildCriteriaArray($filter, $params, $ssFilters);
            /** @var Entity[] $entities */
            $entities = $this->queryEntities($table, $filter, $fields, $extras);
            foreach ($entities as $entity) {
                $entity = static::parseRecordToEntity($record, $entity);
                $this->service->getConnection()->updateEntity($table, $entity);
            }

            $out = static::parseEntitiesToRecords($entities, $fields);

            return $out;
        } catch (\Exception $ex) {
            throw new InternalServerErrorException("Failed to update records in '$table'.\n{$ex->getMessage()}");
        }
    }

    /**
     * {@inheritdoc}
     */
    public function patchRecordsByFilter($table, $record, $filter = null, $params = array(), $extras = array())
    {
        $record = DbUtilities::validateAsArray($record, null, false, 'There are no fields in the record.');

        $fields = ArrayUtils::get($extras, 'fields');
        $ssFilters = ArrayUtils::get($extras, 'ss_filters');
        try {
            // parse filter
            $filter = static::buildCriteriaArray($filter, $params, $ssFilters);
            /** @var Entity[] $entities */
            $entities = $this->queryEntities($table, $filter, $fields, $extras);
            foreach ($entities as $entity) {
                $entity = static::parseRecordToEntity($record, $entity);
                $this->service->getConnection()->mergeEntity($table, $entity);
            }

            $out = static::parseEntitiesToRecords($entities, $fields);

            return $out;
        } catch (\Exception $ex) {
            throw new InternalServerErrorException("Failed to patch records in '$table'.\n{$ex->getMessage()}");
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

        $fields = ArrayUtils::get($extras, 'fields');
        $ssFilters = ArrayUtils::get($extras, 'ss_filters');
        try {
            $filter = static::buildCriteriaArray($filter, $params, $ssFilters);
            /** @var Entity[] $entities */
            $entities = $this->queryEntities($table, $filter, $fields, $extras);
            foreach ($entities as $entity) {
                $partitionKey = $entity->getPartitionKey();
                $rowKey = $entity->getRowKey();
                $this->service->getConnection()->deleteEntity($table, $partitionKey, $rowKey);
            }

            $out = static::parseEntitiesToRecords($entities, $fields);

            return $out;
        } catch (\Exception $ex) {
            throw new InternalServerErrorException("Failed to delete records from '$table'.\n{$ex->getMessage()}");
        }
    }

    /**
     * {@inheritdoc}
     */
    public function retrieveRecordsByFilter($table, $filter = null, $params = array(), $extras = array())
    {
        $fields = ArrayUtils::get($extras, 'fields');
        $ssFilters = ArrayUtils::get($extras, 'ss_filters');

        $options = new QueryEntitiesOptions();
        $options->setSelectFields(array());
        if (!empty($fields) && ('*' != $fields)) {
            $fields = array_map('trim', explode(',', trim($fields, ',')));
            $options->setSelectFields($fields);
        }

        $limit = intval(ArrayUtils::get($extras, 'limit', 0));
        if ($limit > 0) {
            $options->setTop($limit);
        }

        $filter = static::buildCriteriaArray($filter, $params, $ssFilters);
        $out = $this->queryEntities($table, $filter, $fields, $extras, true);

        return $out;
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
        $ids = array(
            array('name' => static::PARTITION_KEY, 'type' => 'string', 'required' => true),
            array('name' => static::ROW_KEY, 'type' => 'string', 'required' => true)
        );

        return $ids;
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
        $options = new QueryEntitiesOptions();
        $options->setSelectFields(array());

        if (!empty($fields) && ('*' != $fields)) {
            if (!is_array($fields)) {
                $fields = array_map('trim', explode(',', trim($fields, ',')));
            }
            $options->setSelectFields($fields);
        }

        $limit = intval(ArrayUtils::get($extras, 'limit', 0));
        if ($limit > 0) {
            $options->setTop($limit);
        }

        if (!empty($parsed_filter)) {
            $query = new QueryStringFilter($parsed_filter);
            $options->setFilter($query);
        }

        try {
            /** @var QueryEntitiesResult $result */
            $result = $this->service->getConnection()->queryEntities($table, $options);

            /** @var Entity[] $entities */
            $entities = $result->getEntities();

            if ($parse_results) {
                return static::parseEntitiesToRecords($entities);
            }

            return $entities;
        } catch (ServiceException $ex) {
            throw new InternalServerErrorException("Failed to filter items from '$table' on Windows Azure Tables service.\n" .
                $ex->getMessage());
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
        $parsed = (empty($fields_info)) ? $record : array();

        unset($parsed['Timestamp']); // not set-able

        if (!empty($fields_info)) {
            $keys = array_keys($record);
            $values = array_values($record);
            foreach ($fields_info as $fieldInfo) {
                $name = ArrayUtils::get($fieldInfo, 'name', '');
                $type = ArrayUtils::get($fieldInfo, 'type');
                $pos = array_search($name, $keys);
                if (false !== $pos) {
                    $fieldVal = ArrayUtils::get($values, $pos);
                    // due to conversion from XML to array, null or empty xml elements have the array value of an empty array
                    if (is_array($fieldVal) && empty($fieldVal)) {
                        $fieldVal = null;
                    }

                    /** validations **/

                    $validations = ArrayUtils::get($fieldInfo, 'validation');

                    if (!static::validateFieldValue($name, $fieldVal, $validations, $for_update, $fieldInfo)) {
                        unset($keys[$pos]);
                        unset($values[$pos]);
                        continue;
                    }

                    $parsed[$name] = $fieldVal;
                    unset($keys[$pos]);
                    unset($values[$pos]);
                }

                // add or override for specific fields
                switch ($type) {
                    case 'timestamp_on_create':
                        if (!$for_update) {
                            $parsed[$name] = new \MongoDate();
                        }
                        break;
                    case 'timestamp_on_update':
                        $parsed[$name] = new \MongoDate();
                        break;
                    case 'user_id_on_create':
                        if (!$for_update) {
                            $userId = 1;//Session::getCurrentUserId();
                            if (isset($userId)) {
                                $parsed[$name] = $userId;
                            }
                        }
                        break;
                    case 'user_id_on_update':
                        $userId = 1;//Session::getCurrentUserId();
                        if (isset($userId)) {
                            $parsed[$name] = $userId;
                        }
                        break;
                }
            }
        }

        if (!empty($filter_info)) {
            $this->validateRecord($parsed, $filter_info, $for_update, $old_record);
        }

        return $parsed;
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
        foreach ($record as $key => $value) {
            if (false === array_search($key, $exclude)) {
                // valid types
//				const DATETIME = 'Edm.DateTime';
//				const BINARY   = 'Edm.Binary';
//				const GUID     = 'Edm.Guid';
                $edmType = EdmType::STRING;
                switch (gettype($value)) {
                    case 'boolean':
                        $edmType = EdmType::BOOLEAN;
                        break;
                    case 'double':
                    case 'float':
                        $edmType = EdmType::DOUBLE;
                        break;
                    case 'integer':
                        $edmType = ($value > 2147483647) ? EdmType::INT64 : EdmType::INT32;
                        break;
                }
                if ($entity->getProperty($key)) {
                    $prop = new Property();
                    $prop->setEdmType($edmType);
                    $prop->setValue($value);
                    $entity->setProperty($key, $prop);
                } else {
                    $entity->addProperty($key, $edmType, $value);
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
        foreach ($entities as $entity) {
            if ($entity instanceof BatchError) {
                /** @var ServiceException $error */
                $error = $entity->getError();
                throw $error;
            }
            if ($entity instanceof InsertEntityResult) {
                /** @var InsertEntityResult $entity */
                $entity = $entity->getEntity();
                $records[] = static::parseEntityToRecord($entity, $include);
            } else {
                $records[] = static::parseEntityToRecord($entity, $include);
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
            $criteria = static::parseFilter($filter, $params);
        } else {
            $criteria = $filter;
        }
        $serverCriteria = static::buildSSFilterArray($ss_filters);
        if (!empty($serverCriteria)) {
            $criteria = (empty($criteria)) ? $serverCriteria : "( $serverCriteria ) and ( $criteria )";
        }

        return $criteria;
    }

    protected static function buildSSFilterArray($ss_filters)
    {
        if (empty($ss_filters)) {
            return '';
        }

        // build the server side criteria
        $filters = ArrayUtils::get($ss_filters, 'filters');
        if (empty($filters)) {
            return '';
        }

        $combiner = ArrayUtils::get($ss_filters, 'filter_op', 'and');
        switch (strtoupper($combiner)) {
            case 'AND':
            case 'OR':
                break;
            default:
                // log and bail
                throw new InternalServerErrorException('Invalid server-side filter configuration detected.');
        }

        $criteria = '';
        foreach ($filters as $filter) {
            $name = ArrayUtils::get($filter, 'name');
            $op = ArrayUtils::get($filter, 'operator');
            if (empty($name) || empty($op)) {
                // log and bail
                throw new InternalServerErrorException('Invalid server-side filter configuration detected.');
            }

            $value = ArrayUtils::get($filter, 'value');
            $value = static::interpretFilterValue($value);

            $temp = static::parseFilter("$name $op $value");
            if (!empty($criteria)) {
                $criteria .= " $combiner ";
            }
            $criteria .= $temp;
        }

        return $criteria;
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
        $search = array(' || ', ' && ', ' OR ', ' AND ', ' NOR ', ' NOT ');
        $replace = array(' or ', ' and ', ' or ', ' and ', ' nor ', ' not ');
        $filter = trim(str_ireplace($search, $replace, ' ' . $filter)); // space added for 'not' case

        // the rest should be comparison operators
        // supported comparison operators are eq, ne, gt, ge, lt, le
        $search =
            array('!=', '>=', '<=', '>', '<', '=', ' EQ ', ' NE ', ' LT ', ' LTE ', ' LE ', ' GT ', ' GTE', ' GE ');
        $replace = array(
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
        $filter = trim(str_ireplace($search, $replace, $filter));

//			WHERE name LIKE "%Joe%"	not supported
//			WHERE name LIKE "%Joe"	not supported
//			WHERE name LIKE "Joe%"	name ge 'Joe' and name lt 'Jof';
//			if ( ( '%' == $val[ strlen( $val ) - 1 ] ) &&
//				 ( '%' != $val[0] ) )
//			{
//			}

        if (!empty($params)) {
            foreach ($params as $name => $value) {
                $filter = str_replace($name, $value, $filter);
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

        $filters = array();
        $filter = '';
        $count = 0;
        foreach ($ids as $id) {
            if (!empty($filter)) {
                $filter .= ' or ';
            }
            $filter .= static::ROW_KEY . " eq '$id'";
            $count++;
            if ($count >= 14) // max comparisons is 15, leave one for partition key
            {
                if (!empty($partition_key)) {
                    $filter = static::PARTITION_KEY . " eq '$partition_key' and ( $filter )";
                }
                $filters[] = $filter;
                $count = 0;
            }
        }

        if (!empty($filter)) {
            if (!empty($partition_key)) {
                $filter = static::PARTITION_KEY . " eq '$partition_key' and ( $filter )";
            }
            $filters[] = $filter;
        }

        return $filters;
    }

    protected function checkForIds(&$record, $ids_info, $extras = null, $on_create = false, $remove = false)
    {
        $id = null;
        if (!empty($ids_info)) {
            if (1 == count($ids_info)) {
                $info = $ids_info[0];
                $name = ArrayUtils::get($info, 'name');
                $value = (is_array($record)) ? ArrayUtils::get($record, $name, null, $remove) : $record;
                if (!empty($value)) {
                    $type = ArrayUtils::get($info, 'type');
                    switch ($type) {
                        case 'int':
                            $value = intval($value);
                            break;
                        case 'string':
                            $value = strval($value);
                            break;
                    }
                    $id = $value;
                } else {
                    $required = ArrayUtils::getBool($info, 'required');
                    // could be passed in as a parameter affecting all records
                    $param = ArrayUtils::get($extras, $name);
                    if ($on_create && $required && empty($param)) {
                        return false;
                    }
                }
            } else {
                $id = array();
                foreach ($ids_info as $info) {
                    $name = ArrayUtils::get($info, 'name');
                    $value = ArrayUtils::get($record, $name, null, $remove);
                    if (!empty($value)) {
                        $type = ArrayUtils::get($info, 'type');
                        switch ($type) {
                            case 'int':
                                $value = intval($value);
                                break;
                            case 'string':
                                $value = strval($value);
                                break;
                        }
                        $id[$name] = $value;
                    } else {
                        $required = ArrayUtils::getBool($info, 'required');
                        // could be passed in as a parameter affecting all records
                        $param = ArrayUtils::get($extras, $name);
                        if ($on_create && $required && empty($param)) {
                            if (!is_array($record) && (static::ROW_KEY == $name)) {
                                $id[$name] = $record;
                            } else {
                                return false;
                            }
                        }
                    }
                }
            }
        }

        if (!empty($id)) {
            return $id;
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
        $ssFilters = ArrayUtils::get($extras, 'ss_filters');
        $fields = ArrayUtils::get($extras, 'fields');
        $fieldsInfo = ArrayUtils::get($extras, 'fields_info');
        $requireMore = ArrayUtils::get($extras, 'require_more');
        $updates = ArrayUtils::get($extras, 'updates');
        $partitionKey = ArrayUtils::get($extras, static::PARTITION_KEY);

        if (!is_array($id)) {
            $id = array(static::ROW_KEY => $id, static::PARTITION_KEY => $partitionKey);
        }
        if (!empty($partitionKey)) {
            $id[static::PARTITION_KEY] = $partitionKey;
        }

        if (!empty($updates)) {
            foreach ($id as $field => $value) {
                if (!isset($updates[$field])) {
                    $updates[$field] = $value;
                }
            }
            $record = $updates;
        } elseif (!empty($record)) {
            if (!empty($partitionKey)) {
                $record[static::PARTITION_KEY] = $partitionKey;
            }
        }

        if (!empty($record)) {
            $forUpdate = false;
            switch ($this->getAction()) {
                case Verbs::PUT:
                case Verbs::MERGE:
                case Verbs::PATCH:
                    $forUpdate = true;
                    break;
            }

            $record = $this->parseRecord($record, $fieldsInfo, $ssFilters, $forUpdate);
            if (empty($record)) {
                throw new BadRequestException('No valid fields were found in record.');
            }

            $entity = static::parseRecordToEntity($record);
        } else {
            $entity = static::parseRecordToEntity($id);
        }

        $partKey = $entity->getPartitionKey();
        if (empty($partKey)) {
            throw new BadRequestException('No valid partition key found in request.');
        }

        $rowKey = $entity->getRowKey();
        if (empty($rowKey)) {
            throw new BadRequestException('No valid row key found in request.');
        }

        // only allow batch if rollback and same partition
        $batch = ($rollback && !empty($partitionKey));
        $out = array();
        switch ($this->getAction()) {
            case Verbs::POST:
                if ($batch) {
                    if (!isset($this->batchOps)) {
                        $this->batchOps = new BatchOperations();
                    }
                    $this->batchOps->addInsertEntity($this->transactionTable, $entity);

                    // track record for output
                    return parent::addToTransaction($record);
                }

                /** @var InsertEntityResult $result */
                $result = $this->service->getConnection()->insertEntity($this->transactionTable, $entity);

                if ($rollback) {
                    $this->addToRollback($entity);
                }

                $out = static::parseEntityToRecord($result->getEntity(), $fields);
                break;
            case Verbs::PUT:
                if ($batch) {
                    if (!isset($this->batchOps)) {
                        $this->batchOps = new BatchOperations();
                    }
                    $this->batchOps->addUpdateEntity($this->transactionTable, $entity);

                    // track record for output
                    return parent::addToTransaction($record);
                }

                if ($rollback) {
                    $old = $this->service->getConnection()->getEntity(
                        $this->transactionTable,
                        $entity->getRowKey(),
                        $entity->getPartitionKey()
                    );
                    $this->addToRollback($old);
                }

                /** @var UpdateEntityResult $result */
                $this->service->getConnection()->updateEntity($this->transactionTable, $entity);

                $out = static::parseEntityToRecord($entity, $fields);
                break;
            case Verbs::MERGE:
            case Verbs::PATCH:
                if ($batch) {
                    if (!isset($this->batchOps)) {
                        $this->batchOps = new BatchOperations();
                    }
                    $this->batchOps->addMergeEntity($this->transactionTable, $entity);

                    // track id for output
                    return parent::addToTransaction(null, $rowKey);
                }

                if ($rollback || $requireMore) {
                    $old = $this->service->getConnection()->getEntity($this->transactionTable, $rowKey, $partKey);
                    if ($rollback) {
                        $this->addToRollback($old);
                    }
                    if ($requireMore) {
                        $out = array_merge(
                            static::parseEntityToRecord($old, $fields),
                            static::parseEntityToRecord($entity, $fields)
                        );
                    }
                }

                $out = (empty($out)) ? static::parseEntityToRecord($entity, $fields) : $out;

                /** @var UpdateEntityResult $result */
                $this->service->getConnection()->mergeEntity($this->transactionTable, $entity);
                break;
            case Verbs::DELETE:
                if ($batch) {
                    if (!isset($this->batchOps)) {
                        $this->batchOps = new BatchOperations();
                    }
                    $this->batchOps->addDeleteEntity($this->transactionTable, $partKey, $rowKey);

                    // track id for output
                    return parent::addToTransaction(null, $rowKey);
                }

                if ($rollback || $requireMore) {
                    $old = $this->service->getConnection()->getEntity($this->transactionTable, $partKey, $rowKey);
                    if ($rollback) {
                        $this->addToRollback($old);
                    }
                    if ($requireMore) {
                        $out = static::parseEntityToRecord($old, $fields);
                    }
                }

                $this->service->getConnection()->deleteEntity($this->transactionTable, $partKey, $rowKey);

                $out = (empty($out)) ? static::parseEntityToRecord($entity, $fields) : $out;
                break;
            case Verbs::GET:
                if (!empty($partitionKey)) {
                    // track id for output
                    return parent::addToTransaction(null, $rowKey);
                }

                /** @var GetEntityResult $result */
                $result = $this->service->getConnection()->getEntity($this->transactionTable, $partKey, $rowKey);

                $out = static::parseEntityToRecord($result->getEntity(), $fields);
                break;
        }

        return $out;
    }

    /**
     * {@inheritdoc}
     */
    protected function commitTransaction($extras = null)
    {
        if (!isset($this->batchOps) && empty($this->batchIds) && empty($this->batchRecords)) {
            return null;
        }

        $fields = ArrayUtils::get($extras, 'fields');
        $partitionKey = ArrayUtils::get($extras, static::PARTITION_KEY);

        $out = array();
        switch ($this->getAction()) {
            case Verbs::POST:
            case Verbs::PUT:
                if (isset($this->batchOps)) {
                    /** @var BatchResult $result */
                    $this->service->getConnection()->batch($this->batchOps);
                }
                if (!empty($this->batchRecords)) {
                    $out = static::parseEntitiesToRecords($this->batchRecords, $fields);
                }
                break;

            case Verbs::MERGE:
            case Verbs::PATCH:
                if (isset($this->batchOps)) {
                    /** @var BatchResult $result */
                    $this->service->getConnection()->batch($this->batchOps);
                }
                if (!empty($this->batchIds)) {
                    $filters = static::buildIdsFilter($this->batchIds, $partitionKey);
                    foreach ($filters as $filter) {
                        $temp = $this->queryEntities($this->transactionTable, $filter, $fields, $extras, true);
                        $out = array_merge($out, $temp);
                    }
                }
                break;

            case Verbs::DELETE:
                if (!empty($this->batchIds)) {
                    $filters = static::buildIdsFilter($this->batchIds, $partitionKey);
                    foreach ($filters as $filter) {
                        $temp = $this->queryEntities($this->transactionTable, $filter, $fields, $extras, true);
                        $out = array_merge($out, $temp);
                    }
                }
                if (isset($this->batchOps)) {
                    /** @var BatchResult $result */
                    $this->service->getConnection()->batch($this->batchOps);
                }
                break;

            case Verbs::GET:
                if (!empty($this->batchIds)) {
                    $filters = static::buildIdsFilter($this->batchIds, $partitionKey);
                    foreach ($filters as $filter) {
                        $temp = $this->queryEntities($this->transactionTable, $filter, $fields, $extras, true);
                        $out = array_merge($out, $temp);
                    }
                }
                break;

            default:
                break;
        }

        $this->batchIds = array();
        $this->batchRecords = array();

        return $out;
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
                    $this->transactionTable,
                    $record->getPartitionKey(),
                    $record->getRowKey()
                );
                break;

            case Verbs::PUT:
            case Verbs::MERGE:
            case Verbs::PATCH:
            case Verbs::DELETE:
                $this->batchOps->addUpdateEntity($this->transactionTable, $record);
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
                    /** @var BatchResult $result */
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