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
    public function describeTable($table, $refresh = true)
    {
        $_name = (is_array($table)) ? ArrayUtils::get($table, 'name') : $table;

        try {
            $_out = array('name' => $_name);
            $_out['access'] = $this->getPermissions($_name);

            return $_out;
        } catch (\Exception $_ex) {
            throw new InternalServerErrorException(
                "Failed to get table properties for table '$_name'.\n{$_ex->getMessage()}"
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
            $_out = array('name' => $table);

            return $_out;
        } catch (\Exception $_ex) {
            throw new InternalServerErrorException("Failed to create table '$table'.\n{$_ex->getMessage()}");
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

//		throw new InternalServerErrorException( "Failed to update table '$_name'." );
        return array('name' => $table);
    }

    /**
     * {@inheritdoc}
     */
    public function deleteTable($table, $check_empty = false)
    {
        $_name = (is_array($table)) ? ArrayUtils::get($table, 'name') : $table;
        if (empty($_name)) {
            throw new BadRequestException('Table name can not be empty.');
        }

        try {
            $this->service->getConnection()->deleteTable($_name);

            return array('name' => $_name);
        } catch (\Exception $_ex) {
            throw new InternalServerErrorException("Failed to delete table '$_name'.\n{$_ex->getMessage()}");
        }
    }
}