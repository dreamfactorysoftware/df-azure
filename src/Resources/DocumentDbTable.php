<?php

namespace DreamFactory\Core\Azure\Resources;

use DreamFactory\Core\Azure\Services\DocumentDB;
use DreamFactory\Core\Database\Schema\Schema;
use DreamFactory\Core\Enums\ApiOptions;
use DreamFactory\Core\Resources\BaseNoSqlDbTableResource;
use DreamFactory\Core\Exceptions\BadRequestException;
use DreamFactory\Core\Utility\DataFormatter;
use DreamFactory\Core\Enums\DbLogicalOperators;
use DreamFactory\Core\Enums\DbComparisonOperators;
use DreamFactory\Core\Exceptions\ForbiddenException;
use DreamFactory\Core\Database\Schema\ColumnSchema;
use DreamFactory\Library\Utility\Enums\Verbs;
use Config;
use DreamFactory\Library\Utility\Scalar;

class DocumentDbTable extends BaseNoSqlDbTableResource
{
    /** ID Field */
    const ID_FIELD = 'id';

    /**
     * @var null|DocumentDB
     */
    protected $parent = null;

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
                $update = array_get($extras, 'updates');
                if (!empty($update)) {
                    foreach ($this->batchIds as $id) {
                        if ($rollback) {
                            $this->rollbackRecords =
                                $this->parent->getConnection()->getDocument($this->transactionTable, $id);
                        }
                        $update[static::ID_FIELD] = $id;
                        $result[] =
                            $this->parent->getConnection()->replaceDocument($this->transactionTable, $update, $id);
                    }
                } else {
                    foreach ($this->batchRecords as $record) {
                        $id = array_get($record, static::ID_FIELD);
                        if ($rollback) {
                            if (!empty($id)) {
                                $this->rollbackRecords =
                                    $this->parent->getConnection()->getDocument($this->transactionTable, $id);
                            }
                        }
                        $result[] =
                            $this->parent->getConnection()->replaceDocument($this->transactionTable, $record, $id);
                    }
                }

                $out = static::cleanRecords($result, static::ID_FIELD);
                break;

            case Verbs::DELETE:
                $result = [];
                foreach ($this->batchIds as $id) {
                    if ($rollback) {
                        $this->rollbackRecords =
                            $this->parent->getConnection()->getDocument($this->transactionTable, $id);
                    }
                    $this->parent->getConnection()->deleteDocument($this->transactionTable, $id);
                    $result[] = ['id' => $id];
                }

                $out = static::cleanRecords($result, $fields, static::ID_FIELD);
                break;

            case Verbs::GET:
                $result = [];
                foreach ($this->batchIds as $id) {
                    $result[] = $this->parent->getConnection()->getDocument($this->transactionTable, $id);
                }
                $out = static::cleanRecords($result, $fields, static::ID_FIELD);

                if (count($this->batchIds) !== count($out)) {
                    throw new BadRequestException('Batch Error: Not all requested ids were found to retrieve.');
                }
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

        if (empty($filter)) {
            $result = $this->parent->getConnection()->listDocuments($table);
            $docs = array_get($result, 'Documents');
            $out = static::cleanRecords($docs, $fields, static::ID_FIELD);
        } else {
            $params = [];
            $columns = [];
            $fieldList = explode(',', $fields);
            if (ApiOptions::FIELDS_ALL === $fields) {
                $result = $this->parent->getConnection()->listDocuments($table);
                $docs = array_get($result, 'Documents');
                $record = $docs[0];
                foreach ($record as $k => $v) {
                    $columns[$k] = new ColumnSchema(['name' => $k]);
                }
            } else {
                foreach ($fieldList as $f) {
                    $columns[$f] = new ColumnSchema(['name' => $f]);
                }
            }
            $filterString = $this->parseFilterString($table, $filter, $params, $columns);
            foreach ($fieldList as $k => $f) {
                $fieldList[$k] = $table . '.' . $f;
            }
            $fields = implode(',', $fieldList);
            $sql = "SELECT " . $fields . " FROM " . $table . " WHERE " . $filterString;
            $result = $this->parent->getConnection()->queryDocument($table, $sql, $params);
            $out = array_get($result, 'Documents');
        }

        if (true === $includeCounts) {
            $out['meta']['count'] = intval(array_get($result, '_count'));
        }

        return $out;
    }

    /**
     * @param string         $filter
     * @param array          $out_params
     * @param ColumnSchema[] $fields_info
     * @param array          $in_params
     *
     * @return string
     * @throws \DreamFactory\Core\Exceptions\BadRequestException
     * @throws \Exception
     */
    protected function parseFilterString($table, $filter, array &$out_params, $fields_info, array $in_params = [])
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
                    $parts = $this->parseFilterString($table, $parts, $out_params, $fields_info, $in_params);

                    return static::localizeOperator($logicalOp) . $parts;
                }
            } else {
                // (a = 1) AND (b = 2) format or (a = 1)AND(b = 2) format
                $filter = str_ireplace(')' . $logicalOp . '(', ') ' . $logicalOp . ' (', $filter);
                $paddedOp = ') ' . $logicalOp . ' (';
                if (false !== $pos = stripos($filter, $paddedOp)) {
                    $left = trim(substr($filter, 0, $pos)) . ')'; // add back right )
                    $right = '(' . trim(substr($filter, $pos + strlen($paddedOp))); // adding back left (
                    $left = $this->parseFilterString($table, $left, $out_params, $fields_info, $in_params);
                    $right = $this->parseFilterString($table, $right, $out_params, $fields_info, $in_params);

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
                if (null === $info = array_get($fields_info, strtolower($field))) {
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

        $key = '@' . $info->getName();
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
}