<?php
namespace DreamFactory\Core\Azure\Resources;

use DreamFactory\Library\Utility\ArrayUtils;
use DreamFactory\Library\Utility\Inflector;
use DreamFactory\Core\Exceptions\BadRequestException;
use DreamFactory\Core\Exceptions\InternalServerErrorException;
use DreamFactory\Core\Resources\BaseNoSqlDbSchemaResource;
use DreamFactory\Core\Utility\DbUtilities;
use DreamFactory\Core\Azure\Services\Table;

class Schema extends BaseNoSqlDbSchemaResource
{
    //*************************************************************************
    //	Members
    //*************************************************************************

    /**
     * @var null|Table
     */
    protected $service = null;

    //*************************************************************************
    //	Methods
    //*************************************************************************

    /**
     * @return null|Table
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
    public function describeTable($table, $refresh = true)
    {
        $name = (is_array($table)) ? ArrayUtils::get($table, 'name') : $table;

        try {
            $out = array('name' => $name);
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
    public function createTable($table, $properties = array(), $check_exist = false, $return_schema = false)
    {
        if (empty($table)) {
            throw new BadRequestException("No 'name' field in data.");
        }

        try {
            $this->service->getConnection()->createTable($table);
            $out = array('name' => $table);

            return $out;
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

//		throw new InternalServerErrorException( "Failed to update table '$name'." );
        return array('name' => $table);
    }

    /**
     * {@inheritdoc}
     */
    public function deleteTable($table, $check_empty = false)
    {
        $name = (is_array($table)) ? ArrayUtils::get($table, 'name') : $table;
        if (empty($name)) {
            throw new BadRequestException('Table name can not be empty.');
        }

        try {
            $this->service->getConnection()->deleteTable($name);

            return array('name' => $name);
        } catch (\Exception $ex) {
            throw new InternalServerErrorException("Failed to delete table '$name'.\n{$ex->getMessage()}");
        }
    }
}