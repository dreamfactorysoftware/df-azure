<?php

namespace DreamFactory\Core\Azure\Resources;

use DreamFactory\Core\Exceptions\DfException;
use DreamFactory\Core\Utility\ResourcesWrapper;
use DreamFactory\Core\Exceptions\InternalServerErrorException;
use DreamFactory\Core\Azure\Components\DocumentDBConnection as Conn;
use DreamFactory\Core\Azure\Services\DocumentDB;
use DreamFactory\Core\Database\Schema\Schema;
use DreamFactory\Core\Enums\ApiOptions;
use DreamFactory\Core\Enums\HttpStatusCodes;
use DreamFactory\Core\Exceptions\NotFoundException;
use DreamFactory\Core\Exceptions\RestException;
use DreamFactory\Core\Resources\BaseNoSqlDbTableResource;
use DreamFactory\Core\Exceptions\BadRequestException;
use DreamFactory\Core\Utility\DataFormatter;
use DreamFactory\Core\Enums\DbLogicalOperators;
use DreamFactory\Core\Enums\DbComparisonOperators;
use DreamFactory\Core\Exceptions\ForbiddenException;
use DreamFactory\Core\Database\Schema\ColumnSchema;
use DreamFactory\Library\Utility\Enums\Verbs;
use DreamFactory\Library\Utility\Scalar;
use Config;

class DocumentDbTable extends BaseNoSqlDbTableResource
{
    /** ID Field */
    const ID_FIELD = 'id';

    /**
     * @var null|DocumentDB
     */
    protected $parent = null;

    /**
     * @var int An internal counter
     */
    private $i = 1;

    /** {@inheritdoc} */
    protected function getIdsInfo(
        $table,
        $fields_info = null,
        &$requested_fields = null,
        $requested_types = null
    ){
        $requested_fields = [static::ID_FIELD]; // can only be this
        $ids = [
            new ColumnSchema(['name' => static::ID_FIELD, 'type' => 'string', 'required' => false]),
        ];

        return $ids;
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
                            $this->parent->getConnection()->deleteDocument($this->transactionTable, $id);
                        }
                    }
                    break;
                case Verbs::PUT:
                case Verbs::PATCH:
                case Verbs::MERGE:
                    foreach ($this->rollbackRecords as $rr) {
                        $id = array_get($rr, static::ID_FIELD);
                        if (!empty($id)) {
                            $this->parent->getConnection()->replaceDocument($this->transactionTable, $rr, $id);
                        }
                    }
                    break;
                case Verbs::DELETE:
                    foreach ($this->rollbackRecords as $rr) {
                        $id = array_get($rr, static::ID_FIELD);
                        if (!empty($id)) {
                            $this->parent->getConnection()->createDocument($this->transactionTable, $rr);
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

    /** {@inheritdoc} */
    protected function commitTransaction($extras = null)
    {
        if (empty($this->batchRecords) && empty($this->batchIds)) {
            return null;
        }
        $rollback = Scalar::boolval(array_get($extras, 'rollback'));
        $continue = Scalar::boolval(array_get($extras, 'continue'));
        $fields = array_get($extras, ApiOptions::FIELDS);

        $out = [];
        switch ($this->getAction()) {
            case Verbs::POST:
                $result = [];
                foreach ($this->batchRecords as $record) {
                    $result[] = $rs = $this->parent->getConnection()->createDocument($this->transactionTable, $record);
                    if ($rollback) {
                        static::addToRollback($rs);
                    }
                }

                $out = static::cleanRecords($result, static::ID_FIELD);
                break;

            case Verbs::PUT:
            case Verbs::MERGE:
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
                                $rs = $this->parent->getConnection()->getDocument($this->transactionTable, $id);
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
                            $this->parent->getConnection()->replaceDocument($this->transactionTable, $record, $id);
                    } catch (\Exception $e) {
                        if (false === $continue && false === $rollback) {
                            throw $e;
                        } else {
                            $result[] = $e->getMessage();
                            $errors[] = (!count($result)) ?: count($result) - 1;

                            if (true === $rollback) {
                                if ($e instanceof DfException) {
                                    $e->setContext(['error' => $errors, ResourcesWrapper::getWrapper() => $result]);
                                    $e->setMessage('Batch Error: Not all records were updated.');
                                }
                                throw $e;
                            }
                        }
                    }
                }

                if (!empty($errors)) {
                    $context = ['error' => $errors, ResourcesWrapper::getWrapper() => $result];
                    throw new BadRequestException('Batch Error: Not all records were updated.', null, null, $context);
                }

                $out = static::cleanRecords($result, static::ID_FIELD);
                break;

            case Verbs::DELETE:
                $result = [];
                $errors = [];
                foreach ($this->batchIds as $id) {
                    if ($rollback) {
                        try {
                            $rs = $this->parent->getConnection()->getDocument($this->transactionTable, $id);
                            static::addToRollback($rs);
                        } catch (RestException $e) {
                            if ($e->getStatusCode() !== HttpStatusCodes::HTTP_NOT_FOUND) {
                                throw $e;
                            }
                        }
                    }

                    try {
                        $this->parent->getConnection()->deleteDocument($this->transactionTable, $id);
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
                                    $e->setMessage('Batch Error: Not all records were deleted.');
                                }
                                throw $e;
                            }
                        }
                    }
                }

                if (!empty($errors)) {
                    $context = ['error' => $errors, ResourcesWrapper::getWrapper() => $result];
                    throw new BadRequestException('Batch Error: Not all records were deleted.', null, null, $context);
                }

                $out = static::cleanRecords($result, $fields, static::ID_FIELD);
                break;

            case Verbs::GET:
                $result = [];
                $errors = [];
                foreach ($this->batchIds as $id) {
                    try {
                        $result[] = $this->parent->getConnection()->getDocument($this->transactionTable, $id);
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
                    throw new NotFoundException('Batch Error: Not all records could be retrieved.', null, null,
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
    public function retrieveRecordsByFilter($table, $filter = null, $params = [], $extras = [])
    {
        $fields = array_get($extras, ApiOptions::FIELDS);
        $includeCounts = Scalar::boolval(array_get($extras, ApiOptions::INCLUDE_COUNT));
        $limit = array_get($extras, 'limit', Config::get('df.db.max_records_returned'));
        $orderBy = $this->cleanOrderBy($table, array_get($extras, 'order_by'));

        if (empty($filter)) {
            if (!empty($orderBy)) {
                $sql = "SELECT * FROM " . $table . ' ORDER BY ' . $orderBy;
                $result =
                    $this->parent->getConnection()->queryDocument($table, $sql, $params, [Conn::OPT_LIMIT => $limit]);
            } else {
                $result = $this->parent->getConnection()->listDocuments($table, [Conn::OPT_LIMIT => $limit]);
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
            $result = $this->parent->getConnection()->queryDocument($table, $sql, $params, [Conn::OPT_LIMIT => $limit]);
            $out = array_get($result, 'Documents');
        }

        if (true === $includeCounts) {
            $out['meta']['count'] = intval(array_get($result, '_count'));
        }

        return $out;
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

            if ((0 === strcmp("'" . trim($value, "'") . "'", $value)) ||
                (0 === strcmp('"' . trim($value, '"') . '"', $value))
            ) {
                $value = substr($value, 1, -1);
            } elseif ((0 === strpos($value, '(')) && ((strlen($value) - 1) === strrpos($value, ')'))) {
                // function call
                return $value;
            }
        }
        // if not already a replacement parameter, evaluate it
        try {
            $value = $this->parseValueForSet($value, $info);
        } catch (ForbiddenException $ex) {
            // need to prop this up?
        }

        switch ($cnvType = Schema::determinePhpConversionType($info->type)) {
            case 'int':
                if (!is_int($value)) {
                    if (!(ctype_digit($value))) {
                        throw new BadRequestException("Field '{$info->getName(true)}' must be a valid integer.");
                    } else {
                        $value = intval($value);
                    }
                }
                break;

            case 'time':
                $cfgFormat = Config::get('df.db_time_format');
                $outFormat = 'H:i:s.u';
                $value = DataFormatter::formatDateTime($outFormat, $value, $cfgFormat);
                break;
            case 'date':
                $cfgFormat = Config::get('df.db_date_format');
                $outFormat = 'Y-m-d';
                $value = DataFormatter::formatDateTime($outFormat, $value, $cfgFormat);
                break;
            case 'datetime':
                $cfgFormat = Config::get('df.db_datetime_format');
                $outFormat = 'Y-m-d H:i:s';
                $value = DataFormatter::formatDateTime($outFormat, $value, $cfgFormat);
                break;
            case 'timestamp':
                $cfgFormat = Config::get('df.db_timestamp_format');
                $outFormat = 'Y-m-d H:i:s';
                $value = DataFormatter::formatDateTime($outFormat, $value, $cfgFormat);
                break;
            case 'bool':
                $value = Scalar::boolval(('false' === trim(strtolower($value))) ? 0 : $value);
                break;

            default:
                break;
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

    /**
     * @param $value
     * @param $field_info
     *
     * @return mixed
     */
    public function parseValueForSet($value, $field_info)
    {
        switch ($field_info->dbType) {
            case 'int':
                return intval($value);
            default:
                return $value;
        }
    }

    /** {@inheritdoc} */
    public function updateRecords($table, $records, $extras = [])
    {
        $records = static::validateAsArray($records, null, true, 'The request contains no valid record sets.');

        $fields = array_get($extras, ApiOptions::FIELDS);
        $idFields = array_get($extras, ApiOptions::ID_FIELD);
        $idTypes = array_get($extras, ApiOptions::ID_TYPE);
        $isSingle = (1 == count($records));
        $rollback = ($isSingle) ? false : Scalar::boolval(array_get($extras, ApiOptions::ROLLBACK, false));
        $continue = ($isSingle) ? false : Scalar::boolval(array_get($extras, ApiOptions::CONTINUES, false));
        if ($rollback && $continue) {
            throw new BadRequestException('Rollback and continue operations can not be requested at the same time.');
        }

        $this->initTransaction($table, $idFields, $idTypes);

        $extras['id_fields'] = $idFields;
        $extras['require_more'] = static::requireMoreFields($fields, $idFields);

        $out = [];
        $errors = [];
        try {
            foreach ($records as $index => $record) {
                try {
                    if (false === $id = static::checkForIds($record, $this->tableIdsInfo, $extras)) {
                        throw new BadRequestException("Required id field(s) not found in record $index: " .
                            print_r($record, true));
                    }

                    $result = $this->addToTransaction($record, $id, $extras, $rollback, $continue, $isSingle);
                    if (isset($result)) {
                        // operation performed, take output
                        $out[$index] = $result;
                    }
                } catch (\Exception $ex) {
                    if ($isSingle || $rollback || !$continue) {
                        if (0 !== $index) {
                            // first error, don't worry about batch just throw it
                            // mark last error and index for batch results
                            $errors[] = $index;
                            $out[$index] = $ex->getMessage();
                        }

                        throw $ex;
                    }

                    // mark error and index for batch results
                    $errors[] = $index;
                    $out[$index] = $ex->getMessage();
                }
            }

            $result = $this->commitTransaction($extras);
            if (isset($result)) {
                $out = $result;
            }

            if (!empty($errors)) {
                throw new BadRequestException();
            }

            return $out;
        } catch (\Exception $ex) {
            $msg = $ex->getMessage();

            $context = null;
            if (!empty($errors)) {
                $wrapper = ResourcesWrapper::getWrapper();
                $context = ['error' => $errors, $wrapper => $out];
                $msg = 'Batch Error: Not all records could be updated.';
            }

            if ($rollback) {
                $this->rollbackTransaction();

                $msg .= " All changes rolled back.";
            }

            if ($ex instanceof RestException) {
                $ex->setMessage($msg);
                throw $ex;
            }

            throw new InternalServerErrorException("Failed to update records in '$table'.\n$msg", null, null, $context);
        }
    }

    /** {@inheritdoc} */
    public function updateRecordsByIds($table, $record, $ids, $extras = [])
    {
        $record = static::validateAsArray($record, null, false, 'There are no fields in the record.');
        $ids = static::validateAsArray($ids, ',', true, 'The request contains no valid identifiers.');

        $fields = array_get($extras, ApiOptions::FIELDS);
        $idFields = array_get($extras, ApiOptions::ID_FIELD);
        $idTypes = array_get($extras, ApiOptions::ID_TYPE);
        $isSingle = (1 == count($ids));
        $rollback = ($isSingle) ? false : Scalar::boolval(array_get($extras, ApiOptions::ROLLBACK, false));
        $continue = ($isSingle) ? false : Scalar::boolval(array_get($extras, ApiOptions::CONTINUES, false));
        if ($rollback && $continue) {
            throw new BadRequestException('Rollback and continue operations can not be requested at the same time.');
        }

        $this->initTransaction($table, $idFields, $idTypes);

        $extras['id_fields'] = $idFields;
        $extras['require_more'] = static::requireMoreFields($fields, $idFields);

        static::removeIds($record, $idFields);
        $extras['updates'] = $record;

        $out = [];
        $errors = [];
        try {
            foreach ($ids as $index => $id) {
                try {
                    if (false === $id = static::checkForIds($id, $this->tableIdsInfo, $extras, true)) {
                        throw new BadRequestException("Required id field(s) not valid in request $index: " .
                            print_r($id, true));
                    }

                    $result = $this->addToTransaction(null, $id, $extras, $rollback, $continue, $isSingle);
                    if (isset($result)) {
                        // operation performed, take output
                        $out[$index] = $result;
                    }
                } catch (\Exception $ex) {
                    if ($isSingle || $rollback || !$continue) {
                        if (0 !== $index) {
                            // first error, don't worry about batch just throw it
                            // mark last error and index for batch results
                            $errors[] = $index;
                            $out[$index] = $ex->getMessage();
                        }

                        throw $ex;
                    }

                    // mark error and index for batch results
                    $errors[] = $index;
                    $out[$index] = $ex->getMessage();
                }
            }

            if (!empty($errors)) {
                throw new BadRequestException();
            }

            $result = $this->commitTransaction($extras);
            if (isset($result)) {
                $out = $result;
            }

            return $out;
        } catch (\Exception $ex) {
            $msg = $ex->getMessage();

            $context = null;
            if (!empty($errors)) {
                $wrapper = ResourcesWrapper::getWrapper();
                $context = ['error' => $errors, $wrapper => $out];
                $msg = 'Batch Error: Not all records could be updated.';
            }

            if ($rollback) {
                $this->rollbackTransaction();

                $msg .= " All changes rolled back.";
            }

            if ($ex instanceof RestException) {
                $ex->setMessage($msg);
                throw $ex;
            }

            throw new InternalServerErrorException("Failed to update records in '$table'.\n$msg", null, null, $context);
        }
    }

    /** {@inheritdoc} */
    public function deleteRecordsByIds($table, $ids, $extras = [])
    {
        $ids = static::validateAsArray($ids, ',', true, 'The request contains no valid identifiers.');

        $fields = array_get($extras, ApiOptions::FIELDS);
        $idFields = array_get($extras, ApiOptions::ID_FIELD);
        $idTypes = array_get($extras, ApiOptions::ID_TYPE);
        $isSingle = (1 == count($ids));
        $rollback = ($isSingle) ? false : Scalar::boolval(array_get($extras, ApiOptions::ROLLBACK, false));
        $continue = ($isSingle) ? false : Scalar::boolval(array_get($extras, ApiOptions::CONTINUES, false));
        if ($rollback && $continue) {
            throw new BadRequestException('Rollback and continue operations can not be requested at the same time.');
        }

        $this->initTransaction($table, $idFields, $idTypes);

        $extras['id_fields'] = $idFields;
        $extras['require_more'] = static::requireMoreFields($fields, $idFields);

        $out = [];
        $errors = [];
        try {
            foreach ($ids as $index => $id) {
                try {
                    if (false === $id = static::checkForIds($id, $this->tableIdsInfo, $extras, true)) {
                        throw new BadRequestException("Required id field(s) not valid in request $index: " .
                            print_r($id, true));
                    }

                    $result = $this->addToTransaction(null, $id, $extras, $rollback, $continue, $isSingle);
                    if (isset($result)) {
                        // operation performed, take output
                        $out[$index] = $result;
                    }
                } catch (\Exception $ex) {
                    if ($isSingle || $rollback || !$continue) {
                        if (0 !== $index) {
                            // first error, don't worry about batch just throw it
                            // mark last error and index for batch results
                            $errors[] = $index;
                            $out[$index] = $ex->getMessage();
                        }

                        throw $ex;
                    }

                    // mark error and index for batch results
                    $errors[] = $index;
                    $out[$index] = $ex->getMessage();
                }
            }

            if (!empty($errors)) {
                throw new BadRequestException();
            }

            $result = $this->commitTransaction($extras);
            if (isset($result)) {
                $out = $result;
            }

            return $out;
        } catch (\Exception $ex) {
            $msg = $ex->getMessage();

            $context = null;
            if (!empty($errors)) {
                $wrapper = ResourcesWrapper::getWrapper();
                $context = ['error' => $errors, $wrapper => $out];
                $msg = 'Batch Error: Not all records could be deleted.';
            }

            if ($rollback) {
                $this->rollbackTransaction();

                $msg .= " All changes rolled back.";
            }

            if ($ex instanceof RestException) {
                $ex->setMessage($msg);
                throw $ex;
            }

            throw new InternalServerErrorException("Failed to delete records from '$table'.\n$msg", null, null,
                $context);
        }
    }

    /** {@inheritdoc} */
    protected function handlePatch()
    {
        return false;
    }
}