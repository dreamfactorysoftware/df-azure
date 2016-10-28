<?php

namespace DreamFactory\Core\Azure\Resources;

use DreamFactory\Core\Resources\BaseNoSqlDbTableResource;

class DocumentDbTable extends BaseNoSqlDbTableResource
{
    protected function getIdsInfo(
        $table,
        $fields_info = null,
        &$requested_fields = null,
        $requested_types = null
    ){
        // TODO: Implement getIdsInfo() method.
    }

    protected function rollbackTransaction()
    {
        // TODO: Implement rollbackTransaction() method.
    }

    protected function commitTransaction($extras = null)
    {
        // TODO: Implement commitTransaction() method.
    }

    public function retrieveRecordsByFilter($table, $filter = null, $params = [], $extras = [])
    {
        // TODO: Implement retrieveRecordsByFilter() method.
    }
}