<?php

namespace DreamFactory\Core\Azure\Models;

use DreamFactory\Core\Database\Components\SupportsExtraDbConfigs;
use DreamFactory\Core\Models\BaseServiceConfigModel;

class DocumentDbConfig extends BaseServiceConfigModel
{
    use SupportsExtraDbConfigs;

    /** @var string  */
    protected $table = 'documentdb_config';

    /** @var array  */
    protected $fillable = ['service_id', 'uri', 'key', 'database'];

    /** @var array  */
    protected $encrypted = ['key'];

    /** @var array  */
    protected $protected = ['key'];

    /** @var array  */
    protected $rules = [
            'uri' => 'required',
            'key' => 'required'
    ];

    /**
     * @param array $schema
     */
    protected static function prepareConfigSchemaField(array &$schema)
    {
        parent::prepareConfigSchemaField($schema);

        switch ($schema['name']) {
            case 'uri':
                $schema['label'] = 'URI';
                $schema['description'] = 'Azure DocumentDB endpoint.';
                break;
            case 'key':
                $schema['description'] = 'Azure DocumentDB key.';
                break;
            case 'database':
                $schema['description'] = 'Azure DocumentDB database';
                break;
        }
    }
}