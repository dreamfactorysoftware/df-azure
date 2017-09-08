<?php

namespace DreamFactory\Core\Azure\Resources;

use DreamFactory\Core\Azure\Components\DocumentDBConnection;
use DreamFactory\Core\Azure\Services\DocumentDB;
use DreamFactory\Core\Database\Resources\BaseNoSqlDbTableResource;
use DreamFactory\Core\Database\Schema\ColumnSchema;
use DreamFactory\Core\Enums\ApiOptions;
use DreamFactory\Core\Enums\HttpStatusCodes;
use DreamFactory\Core\Enums\DbLogicalOperators;
use DreamFactory\Core\Enums\DbComparisonOperators;
use DreamFactory\Core\Enums\Verbs;
use DreamFactory\Core\Exceptions\BadRequestException;
use DreamFactory\Core\Exceptions\DfException;
use DreamFactory\Core\Exceptions\ForbiddenException;
use DreamFactory\Core\Exceptions\InternalServerErrorException;
use DreamFactory\Core\Exceptions\NotFoundException;
use DreamFactory\Core\Exceptions\RestException;
use DreamFactory\Core\Utility\ResourcesWrapper;

class DocumentDbTable extends BaseNoSqlDbTableResource
{
    //*************************************************************************
    //	Constants
    //*************************************************************************

    /** ID Field */
    const ID_FIELD = 'id';

    //*************************************************************************
    //	Members
    //*************************************************************************

    /**
     * @var null|DocumentDB
     */
    protected $parent = null;

    /**
     * @var int An internal counter
     */
    private $i = 1;

    /**
     * {@inheritdoc}
     */
    public function updateRecordsByFilter($table, $record, $filter = null, $params = [], $extras = [], $patch = false)
    {
        $record = static::validateAsArray($record, null, false, 'There are no fields in the record.');

        $fields = array_get($extras, ApiOptions::FIELDS);
        $idFields = array_get($extras, ApiOptions::ID_FIELD);
        $idTypes = array_get($extras, ApiOptions::ID_TYPE);

        // slow, but workable for now, maybe faster than merging individuals
        $extras[ApiOptions::FIELDS] = ApiOptions::FIELDS_ALL;
        $records = $this->retrieveRecordsByFilter($table, $filter, $params, $extras);
        unset($records['meta']);

        $fieldsInfo = $this->getFieldsInfo($table);
        $idsInfo = $this->getIdsInfo($table, $fieldsInfo, $idFields, $idTypes);
        if (empty($idsInfo)) {
            throw new InternalServerErrorException("Identifying field(s) could not be determined.");
        }

        $ids = $this->recordsAsIds($records, $idsInfo);
        if (empty($ids)) {
            return [];
        }

        $extras[ApiOptions::FIELDS] = $fields;

        if (true === $patch) {
            $newRecords = [];
            foreach ($ids as $id) {
                $record[static::ID_FIELD] = $id;
                $newRecords[] = $record;
            }
            $newRecords = $this->mergeRecords($newRecords);

            return $this->updateRecords($this->transactionTable, $newRecords, $extras);
        } else {
            return $this->updateRecordsByIds($table, $record, $ids, $extras);
        }
    }

    /**
     * {@inheritdoc}
     */
    public function retrieveRecordsByFilter($table, $filter = null, $params = [], $extras = [])
    {
        $fields = array_get($extras, ApiOptions::FIELDS);
        $includeCounts = array_get_bool($extras, ApiOptions::INCLUDE_COUNT);
        $limit = array_get($extras, 'limit', static::getMaxRecordsReturnedLimit());
        $orderBy = $this->cleanOrderBy($table, array_get($extras, 'order_by'));

        if (empty($filter)) {
            if (!empty($orderBy)) {
                /** @noinspection SqlDialectInspection */
                /** @noinspection SqlNoDataSourceInspection */
                $sql = "SELECT * FROM " . $table . ' ORDER BY ' . $orderBy;
                $result =
                    $this->getConnection()->queryDocument($table, $sql, $params, [Conn::OPT_LIMIT => $limit]);
            } else {
                $result = $this->getConnection()->listDocuments($table, [Conn::OPT_LIMIT => $limit]);
            }
            $docs = array_get($result, 'Documents');
            $out = static::cleanRecords($docs, $fields, static::ID_FIELD);
        } else {
            $params = [];
            $fieldList = explode(',', $fields);
            $this->i = 1;
            $filterString = $this->parseFilterString($table, $filter, $params);
            if ($fields !== ApiOptions::FIELDS_ALL) {
                foreach ($fieldList as $k => $f) {
                    $fieldList[$k] = $table . '.' . $f;
                }
                $fields = implode(',', $fieldList);
            }
            $sql = "SELECT " . $fields . " FROM " . $table . " WHERE " . $filterString;
            if (!empty($orderBy)) {
                $sql .= ' ORDER BY ' . $orderBy;
            }
            $result = $this->getConnection()->queryDocument($table, $sql, $params, [Conn::OPT_LIMIT => $limit]);
            $out = array_get($result, 'Documents');
        }

        if (true === $includeCounts) {
            $out['meta']['count'] = intval(array_get($result, '_count'));
        }

        return $out;
    }

    /**
     * @return DocumentDBConnection
     */
    protected function getConnection()
    {
        return $this->parent->getConnection();
    }

    /**
     * {@inheritdoc}
     */
    protected function getIdsInfo($table, $fields_info = null, &$requested_fields = null, $requested_types = null)
    {
        $requested_fields = [static::ID_FIELD]; // can only be this
        $ids = [
            new ColumnSchema(['name' => static::ID_FIELD, 'type' => 'string', 'required' => false]),
        ];

        return $ids;
    }

    /**
     * @inheritdoc
     */
    protected function addToTransaction(
        $record = null,
        $id = null,
        $extras = null,
        $rollback = false,
        $continue = false,
        $single = false
    ) {
        $fields = array_get($extras, ApiOptions::FIELDS);

        $out = [];
        switch ($this->getAction()) {
            case Verbs::POST:
                $result = $rs = $this->getConnection()->createDocument($this->transactionTable, $record);
                if ($rollback) {
                    static::addToRollback($rs);
                }

                $out = static::cleanRecord($result, static::ID_FIELD);
                break;

            case Verbs::PUT:
            case Verbs::PATCH:
                if (!empty($update = array_get($extras, 'updates'))) {
                    if (!empty($update)) {
                        $update[static::ID_FIELD] = $id;
                        $record = $update;
                    }
                }

                $id = array_get($record, static::ID_FIELD);
                if ($rollback) {
                    if (!empty($id)) {
                        try {
                            $rs = $this->getConnection()->getDocument($this->transactionTable, $id);
                            static::addToRollback($rs);
                        } catch (RestException $e) {
                            if ($e->getStatusCode() !== HttpStatusCodes::HTTP_NOT_FOUND) {
                                throw $e;
                            }
                        }
                    }
                }

                try {
                    $result =
                        $this->getConnection()->replaceDocument($this->transactionTable, $record, $id);
                } catch (\Exception $e) {
                    if (false === $continue && false === $rollback) {
                        throw $e;
                    } else {
                        $result[] = $e->getMessage();
                        $errors[] = (!count($result)) ?: count($result) - 1;

                        if (true === $rollback) {
                            if ($e instanceof DfException) {
                                $e->setContext(['error' => $errors, ResourcesWrapper::getWrapper() => $result]);
                                $e->setMessage('Batch Error: Not all requested records could be updated.');
                            }
                            throw $e;
                        }
                    }
                }

                if (!empty($errors)) {
                    $context = ['error' => $errors, ResourcesWrapper::getWrapper() => $result];
                    throw new BadRequestException('Batch Error: Not all requested records could be updated.', null,
                        null, $context);
                }

                $out = static::cleanRecord($result, static::ID_FIELD);
                break;

            case Verbs::DELETE:
                if ($rollback) {
                    try {
                        $result = $this->getConnection()->getDocument($this->transactionTable, $id);
                        static::addToRollback($result);
                    } catch (RestException $e) {
                        if ($e->getStatusCode() !== HttpStatusCodes::HTTP_NOT_FOUND) {
                            throw $e;
                        }
                    }
                }

                try {
                    $this->getConnection()->deleteDocument($this->transactionTable, $id);
                    $result[] = ['id' => $id];
                } catch (\Exception $e) {
                    if (false === $continue && false === $rollback) {
                        throw $e;
                    } else {
                        $result[] = $e->getMessage();
                        $errors[] = (!count($result)) ?: count($result) - 1;

                        if (true === $rollback) {
                            if ($e instanceof DfException) {
                                $e->setContext(['error' => $errors, ResourcesWrapper::getWrapper() => $result]);
                                $e->setMessage('Batch Error: Not all requested records could be deleted.');
                            }
                            throw $e;
                        }
                    }
                }

                $out = static::cleanRecord($result, $fields, static::ID_FIELD);
                break;

            case Verbs::GET:
                try {
                    $result = $this->getConnection()->getDocument($this->transactionTable, $id);
                } catch (RestException $e) {
                    if ($e->getStatusCode() == HttpStatusCodes::HTTP_NOT_FOUND) {
                        throw new NotFoundException("Record with id '$id' not found.");
                    } else {
                        throw $e;
                    }
                }

                $out = static::cleanRecord($result, $fields, static::ID_FIELD);
                break;
        }

        return $out;
    }

    /**
     * {@inheritdoc}
     */
    protected function commitTransaction($extras = null)
    {
        if (empty($this->batchRecords) && empty($this->batchIds)) {
            return null;
        }
        $rollback = array_get_bool($extras, 'rollback');
        $continue = array_get_bool($extras, 'continue');
        $fields = array_get($extras, ApiOptions::FIELDS);

        $out = [];
        switch ($this->getAction()) {
            case Verbs::POST:
                $result = [];
                foreach ($this->batchRecords as $record) {
                    $result[] = $rs = $this->getConnection()->createDocument($this->transactionTable, $record);
                    if ($rollback) {
                        static::addToRollback($rs);
                    }
                }

                $out = static::cleanRecords($result, static::ID_FIELD);
                break;

            case Verbs::PUT:
            case Verbs::PATCH:
                $result = [];
                $errors = [];
                $records = $this->batchRecords;
                $update = array_get($extras, 'updates');

                if (!empty($update)) {
                    foreach ($this->batchIds as $id) {
                        $update[static::ID_FIELD] = $id;
                        $records[] = $update;
                    }
                }

                foreach ($records as $record) {
                    $id = array_get($record, static::ID_FIELD);
                    if ($rollback) {
                        if (!empty($id)) {
                            try {
                                $rs = $this->getConnection()->getDocument($this->transactionTable, $id);
                                static::addToRollback($rs);
                            } catch (RestException $e) {
                                if ($e->getStatusCode() !== HttpStatusCodes::HTTP_NOT_FOUND) {
                                    throw $e;
                                }
                            }
                        }
                    }

                    try {
                        $result[] =
                            $this->getConnection()->replaceDocument($this->transactionTable, $record, $id);
                    } catch (\Exception $e) {
                        if (false === $continue && false === $rollback) {
                            throw $e;
                        } else {
                            $result[] = $e->getMessage();
                            $errors[] = (!count($result)) ?: count($result) - 1;

                            if (true === $rollback) {
                                if ($e instanceof DfException) {
                                    $e->setContext(['error' => $errors, ResourcesWrapper::getWrapper() => $result]);
                                    $e->setMessage('Batch Error: Not all requested records could be updated.');
                                }
                                throw $e;
                            }
                        }
                    }
                }

                if (!empty($errors)) {
                    $context = ['error' => $errors, ResourcesWrapper::getWrapper() => $result];
                    throw new BadRequestException('Batch Error: Not all requested records could be updated.', null,
                        null, $context);
                }

                $out = static::cleanRecords($result, static::ID_FIELD);
                break;

            case Verbs::DELETE:
                $result = [];
                $errors = [];
                foreach ($this->batchIds as $id) {
                    if ($rollback) {
                        try {
                            $rs = $this->getConnection()->getDocument($this->transactionTable, $id);
                            static::addToRollback($rs);
                        } catch (RestException $e) {
                            if ($e->getStatusCode() !== HttpStatusCodes::HTTP_NOT_FOUND) {
                                throw $e;
                            }
                        }
                    }

                    try {
                        $this->getConnection()->deleteDocument($this->transactionTable, $id);
                        $result[] = ['id' => $id];
                    } catch (\Exception $e) {
                        if (false === $continue && false === $rollback) {
                            throw $e;
                        } else {
                            $result[] = $e->getMessage();
                            $errors[] = (!count($result)) ?: count($result) - 1;

                            if (true === $rollback) {
                                if ($e instanceof DfException) {
                                    $e->setContext(['error' => $errors, ResourcesWrapper::getWrapper() => $result]);
                                    $e->setMessage('Batch Error: Not all requested records could be deleted.');
                                }
                                throw $e;
                            }
                        }
                    }
                }

                if (!empty($errors)) {
                    $context = ['error' => $errors, ResourcesWrapper::getWrapper() => $result];
                    throw new BadRequestException('Batch Error: Not all requested records could be deleted.', null,
                        null, $context);
                }

                $out = static::cleanRecords($result, $fields, static::ID_FIELD);
                break;

            case Verbs::GET:
                $result = [];
                $errors = [];
                foreach ($this->batchIds as $id) {
                    try {
                        $result[] = $this->getConnection()->getDocument($this->transactionTable, $id);
                    } catch (RestException $e) {
                        if ($e->getStatusCode() == HttpStatusCodes::HTTP_NOT_FOUND && count($this->batchIds) > 1) {
                            $result[] = "Record with identifier '" . $id . "' not found.";
                            $errors[] = (!count($result)) ?: count($result) - 1;
                        } else {
                            throw $e;
                        }
                    }
                }

                if (!empty($errors)) {
                    $context = ['error' => $errors, ResourcesWrapper::getWrapper() => $result];
                    throw new NotFoundException('Batch Error: Not all requested records could be retrieved.', null,
                        null,
                        $context);
                }

                $out = static::cleanRecords($result, $fields, static::ID_FIELD);
                break;

            default:
                break;
        }

        $this->batchIds = [];
        $this->batchRecords = [];

        return $out;
    }

    /** {@inheritdoc} */
    protected function rollbackTransaction()
    {
        if (!empty($this->rollbackRecords)) {
            switch ($this->getAction()) {
                case Verbs::POST:
                    foreach ($this->rollbackRecords as $rr) {
                        $id = array_get($rr, static::ID_FIELD);
                        if (!empty($id)) {
                            $this->getConnection()->deleteDocument($this->transactionTable, $id);
                        }
                    }
                    break;
                case Verbs::PUT:
                case Verbs::PATCH:
                    foreach ($this->rollbackRecords as $rr) {
                        $id = array_get($rr, static::ID_FIELD);
                        if (!empty($id)) {
                            $this->getConnection()->replaceDocument($this->transactionTable, $rr, $id);
                        }
                    }
                    break;
                case Verbs::DELETE:
                    foreach ($this->rollbackRecords as $rr) {
                        $id = array_get($rr, static::ID_FIELD);
                        if (!empty($id)) {
                            $this->getConnection()->createDocument($this->transactionTable, $rr);
                        }
                    }
                    break;
                default:
                    // nothing to do here, rollback handled on bulk calls
                    break;
            }

            $this->rollbackRecords = [];
        }

        return true;
    }

    /**
     * Cleans order by clause
     *
     * @param $table
     * @param $orderBy
     *
     * @return string
     */
    private function cleanOrderBy($table, $orderBy)
    {
        if (!empty($orderBy)) {
            $orderByArray = explode(',', $orderBy);
            foreach ($orderByArray as $k => $order) {
                $orderByArray[$k] = $table . '.' . $order;
            }
            $orderBy = implode(',', $orderByArray);
        }

        return $orderBy;
    }

    /**
     * @param string $table
     * @param string $filter
     * @param array  $out_params
     * @param array  $in_params
     *
     * @return string
     * @throws \DreamFactory\Core\Exceptions\BadRequestException
     * @throws \Exception
     */
    protected function parseFilterString($table, $filter, array &$out_params, array $in_params = [])
    {
        if (empty($filter)) {
            return null;
        }

        $filter = trim($filter);
        // todo use smarter regex
        // handle logical operators first
        $logicalOperators = DbLogicalOperators::getDefinedConstants();
        foreach ($logicalOperators as $logicalOp) {
            if (DbLogicalOperators::NOT_STR === $logicalOp) {
                // NOT(a = 1)  or NOT (a = 1)format
                if ((0 === stripos($filter, $logicalOp . ' (')) || (0 === stripos($filter, $logicalOp . '('))) {
                    $parts = trim(substr($filter, 3));
                    $parts = $this->parseFilterString($table, $parts, $out_params, $in_params);

                    return static::localizeOperator($logicalOp) . $parts;
                }
            } else {
                // (a = 1) AND (b = 2) format or (a = 1)AND(b = 2) format
                $filter = str_ireplace(')' . $logicalOp . '(', ') ' . $logicalOp . ' (', $filter);
                $paddedOp = ') ' . $logicalOp . ' (';
                if (false !== $pos = stripos($filter, $paddedOp)) {
                    $left = trim(substr($filter, 0, $pos)) . ')'; // add back right )
                    $right = '(' . trim(substr($filter, $pos + strlen($paddedOp))); // adding back left (
                    $left = $this->parseFilterString($table, $left, $out_params, $in_params);
                    $right = $this->parseFilterString($table, $right, $out_params, $in_params);

                    return $left . ' ' . static::localizeOperator($logicalOp) . ' ' . $right;
                }
            }
        }

        $wrap = false;
        if ((0 === strpos($filter, '(')) && ((strlen($filter) - 1) === strrpos($filter, ')'))) {
            // remove unnecessary wrapping ()
            $filter = substr($filter, 1, -1);
            $wrap = true;
        }

        // Some scenarios leave extra parens dangling
        $pure = trim($filter, '()');
        $pieces = explode($pure, $filter);
        $leftParen = (!empty($pieces[0]) ? $pieces[0] : null);
        $rightParen = (!empty($pieces[1]) ? $pieces[1] : null);
        $filter = $pure;

        // the rest should be comparison operators
        // Note: order matters here!
        $sqlOperators = DbComparisonOperators::getParsingOrder();
        foreach ($sqlOperators as $sqlOp) {
            $paddedOp = static::padOperator($sqlOp);
            if (false !== $pos = stripos($filter, $paddedOp)) {
                $field = trim(substr($filter, 0, $pos));
                $negate = false;
                if (false !== strpos($field, ' ')) {
                    $parts = explode(' ', $field);
                    $partsCount = count($parts);
                    if (($partsCount > 1) &&
                        (0 === strcasecmp($parts[$partsCount - 1], trim(DbLogicalOperators::NOT_STR)))
                    ) {
                        // negation on left side of operator
                        array_pop($parts);
                        $field = implode(' ', $parts);
                        $negate = true;
                    }
                }
                /** @type ColumnSchema $info */
                if (null === $info = new ColumnSchema(['name' => strtolower($field)])) {
                    // This could be SQL injection attempt or bad field
                    throw new BadRequestException("Invalid or unparsable field in filter request: '$field'");
                }

                // make sure we haven't chopped off right side too much
                $value = trim(substr($filter, $pos + strlen($paddedOp)));
                if ((0 !== strpos($value, "'")) &&
                    (0 !== $lpc = substr_count($value, '(')) &&
                    ($lpc !== $rpc = substr_count($value, ')'))
                ) {
                    // add back to value from right
                    $parenPad = str_repeat(')', $lpc - $rpc);
                    $value .= $parenPad;
                    $rightParen = preg_replace('/\)/', '', $rightParen, $lpc - $rpc);
                }
                if (DbComparisonOperators::requiresValueList($sqlOp)) {
                    if ((0 === strpos($value, '(')) && ((strlen($value) - 1) === strrpos($value, ')'))) {
                        // remove wrapping ()
                        $value = substr($value, 1, -1);
                        $parsed = [];
                        foreach (explode(',', $value) as $each) {
                            $parsed[] = $this->parseFilterValue(trim($each), $info, $out_params, $in_params);
                        }
                        $value = '(' . implode(',', $parsed) . ')';
                    } else {
                        throw new BadRequestException('Filter value lists must be wrapped in parentheses.');
                    }
                } elseif (DbComparisonOperators::requiresNoValue($sqlOp)) {
                    $value = null;
                } else {
                    static::modifyValueByOperator($sqlOp, $value);
                    $value = $this->parseFilterValue($value, $info, $out_params, $in_params);
                }

                $sqlOp = static::localizeOperator($sqlOp);
                if ($negate) {
                    $sqlOp = DbLogicalOperators::NOT_STR . ' ' . $sqlOp;
                }

                $out = $table . '.' . $info->name . " $sqlOp";
                $out .= (isset($value) ? " $value" : null);
                if ($leftParen) {
                    $out = $leftParen . $out;
                }
                if ($rightParen) {
                    $out .= $rightParen;
                }

                return ($wrap ? '(' . $out . ')' : $out);
            }
        }

        // This could be SQL injection attempt or unsupported filter arrangement
        throw new BadRequestException('Invalid or unparsable filter request.');
    }

    /**
     * @param mixed        $value
     * @param ColumnSchema $info
     * @param array        $out_params
     * @param array        $in_params
     *
     * @return int|null|string
     * @throws BadRequestException
     */
    protected function parseFilterValue($value, ColumnSchema $info, array &$out_params, array $in_params = [])
    {
        // if a named replacement parameter, un-name it because Laravel can't handle named parameters
        if (is_array($in_params) && (0 === strpos($value, ':'))) {
            if (array_key_exists($value, $in_params)) {
                $value = $in_params[$value];
            }
        }

        // remove quoting on strings if used, i.e. 1.x required them
        if (is_string($value)) {

            if ((0 === strpos($value, '(')) && ((strlen($value) - 1) === strrpos($value, ')'))) {
                // function call
                return $value;
            }
        }
        // if not already a replacement parameter, evaluate it
        try {
            switch ($info->dbType) {
                case 'int':
                    $value = intval($value);
                    break;
            }
        } catch (ForbiddenException $ex) {
            // need to prop this up?
        }

        if (is_numeric($value)) {
            $value = (int)$value;
        } elseif ('true' === strtolower($value)) {
            $value = true;
        } elseif ('false' === strtolower($value)) {
            $value = false;
        } elseif ((0 === strcmp("'" . trim($value, "'") . "'", $value)) ||
            (0 === strcmp('"' . trim($value, '"') . '"', $value))
        ) {
            $value = substr($value, 1, -1);
        }

        $key = '@' . $info->getName() . $this->i;
        $this->i++;
        $out_params[] = [
            'name'  => $key,
            'value' => $value
        ];
        $value = $key;

        return $value;
    }


    /** {@inheritdoc} */
    protected function handlePatch()
    {
        if (empty($this->resource)) {
            // not currently supported, maybe batch opportunity?
            return false;
        }

        if (false === ($tableName = $this->doesTableExist($this->resource, true))) {
            throw new NotFoundException('Table "' . $this->resource . '" does not exist in the database.');
        }
        $this->transactionTable = $tableName;

        $options = $this->request->getParameters();

        if (!is_null($this->resourceId)) {
            $record = $this->getPayloadData();
            $record[static::ID_FIELD] = $this->resourceId;
            $record = array_get($this->mergeRecords([$record]), 0);

            return $this->updateRecordById($tableName, $record, $this->resourceId, $options);
        }

        $records = ResourcesWrapper::unwrapResources($this->getPayloadData());
        if (empty($records)) {
            throw new BadRequestException('No record(s) detected in request.' . ResourcesWrapper::getWrapperMsg());
        }

        $ids = array_get($options, ApiOptions::IDS);

        if (!empty($ids)) {
            $record = array_get($records, 0, $records);
            $newRecords = [];
            foreach (explode(',', $ids) as $id) {
                $record[static::ID_FIELD] = $id;
                $newRecords[] = $record;
            }
            $newRecords = $this->mergeRecords($newRecords);
            $result = $this->updateRecords($tableName, $newRecords, $options);
        } else {
            $filter = array_get($options, ApiOptions::FILTER);
            if (!empty($filter)) {
                $record = array_get($records, 0, $records);
                $params = array_get($options, ApiOptions::PARAMS, []);
                $result = $this->updateRecordsByFilter(
                    $tableName,
                    $record,
                    $filter,
                    $params,
                    $options,
                    true
                );
            } else {
                $records = $this->mergeRecords($records);
                $result = $this->updateRecords($tableName, $records, $options);
            }
        }

        $meta = array_get($result, 'meta');
        unset($result['meta']);

        $asList = $this->request->getParameterAsBool(ApiOptions::AS_LIST);
        $idField = $this->request->getParameter(ApiOptions::ID_FIELD, static::getResourceIdentifier());
        $result = ResourcesWrapper::cleanResources($result, $asList, $idField, ApiOptions::FIELDS_ALL, !empty($meta));

        if (!empty($meta)) {
            $result['meta'] = $meta;
        }

        return $result;
    }

    /**
     * Merges new record with existing record to perform PATCH operation
     *
     * @param array $records
     *
     * @return array
     * @throws \DreamFactory\Core\Exceptions\InternalServerErrorException
     */
    protected function mergeRecords(array $records)
    {
        foreach ($records as $key => $record) {
            if (null === $id = array_get($record, static::ID_FIELD)) {
                throw new InternalServerErrorException('No ' .
                    static::ID_FIELD .
                    ' field found in supplied record(s). Cannot merge record(s) for PATCH operation.');
            }

            $rs = $this->getConnection()->getDocument($this->transactionTable, $id);
            $record = array_merge($rs, $record);
            $records[$key] = $record;
        }

        return $records;
    }
}