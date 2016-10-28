<?php

namespace DreamFactory\Core\Azure\Resources;

use DreamFactory\Core\Azure\Services\DocumentDB;
use DreamFactory\Core\Resources\BaseNoSqlDbSchemaResource;
use DreamFactory\Core\Exceptions\InternalServerErrorException;
use DreamFactory\Core\Exceptions\BadRequestException;

class DocumentDbSchema extends BaseNoSqlDbSchemaResource
{
    /**
     * @var null|DocumentDB
     */
    protected $parent = null;

    /**
     * @return null|DocumentDB
     */
    public function getService()
    {
        return $this->parent;
    }

    /**
     * {@inheritdoc}
     */
    public function describeTable($table, $refresh = true)
    {
        $name = (is_array($table)) ? array_get($table, 'name') : $table;

        try {
            $out = $this->parent->getConnection()->getCollection($name);
            $out['name'] = $name;
            $out['access'] = $this->getPermissions($name);

            return $out;
        } catch (\Exception $ex) {
            throw new InternalServerErrorException(
                "Failed to get table properties for table '$name'.\n{$ex->getMessage()}"
            );
        }
    }

    /**
     * {@inheritdoc}
     */
    public function createTables($tables, $check_exist = false, $return_schema = false)
    {
        $tables = static::validateAsArray(
            $tables,
            ',',
            true,
            'The request contains no valid table names or properties.'
        );

        $out = [];
        foreach ($tables as $table) {
            $name = (is_array($table)) ? array_get($table, 'name', array_get($table, 'id')) : $table;
            $out[] = $this->createTable($name, $table, $check_exist, $return_schema);
        }

        return $out;
    }

    /**
     * {@inheritdoc}
     */
    public function createTable($table, $properties = array(), $check_exist = false, $return_schema = false)
    {
        if (empty($table)) {
            throw new BadRequestException("No 'name' field in data.");
        }

        try {
            $data = ['id' => $table];
            if(isset($properties['indexingPolicy'])){
                $data['indexingPolicy'] = $properties['indexingPolicy'];
            }
            if(isset($properties['partitionKey'])){
                $data['partitionKey'] = $properties['partitionKey'];
            }

            $this->parent->getConnection()->createCollection($data);
            $this->refreshCachedTables();
            return ['name' => $table];
        } catch (\Exception $ex) {
            throw new InternalServerErrorException("Failed to create table '$table'.\n{$ex->getMessage()}");
        }
    }

    /**
     * {@inheritdoc}
     */
    public function updateTable($table, $properties = array(), $allow_delete_fields = false, $return_schema = false)
    {
        if (empty($table)) {
            throw new BadRequestException("No 'name' field in data.");
        }

        $data = ['id' => $table];
        if(isset($properties['indexingPolicy'])){
            $data['indexingPolicy'] = $properties['indexingPolicy'];
        }
        $this->parent->getConnection()->replaceCollection($data, $table);
        $this->refreshCachedTables();

        return ['name' => $table];
    }

    /**
     * {@inheritdoc}
     */
    public function deleteTable($table, $check_empty = false)
    {
        $name = (is_array($table)) ? array_get($table, 'name') : $table;
        if (empty($name)) {
            throw new BadRequestException('Table name can not be empty.');
        }

        try {
            $this->parent->getConnection()->deleteCollection($name);
            $this->refreshCachedTables();

            return ['name' => $name];
        } catch (\Exception $ex) {
            throw new InternalServerErrorException("Failed to delete table '$name'.\n{$ex->getMessage()}");
        }
    }
}