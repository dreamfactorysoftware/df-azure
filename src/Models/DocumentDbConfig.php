<?php

namespace DreamFactory\Core\Azure\Models;

use DreamFactory\Core\Models\BaseServiceConfigModel;
use DreamFactory\Core\Exceptions\BadRequestException;

class DocumentDbConfig extends BaseServiceConfigModel
{
    protected $table = 'documentdb_config';

    protected $fillable = ['service_id', 'uri', 'key', 'database'];

    protected $encrypted = ['key'];

    protected $protected = ['key'];

    /**
     * {@inheritdoc}
     */
    public static function validateConfig($config, $create = true)
    {
        $validator = static::makeValidator($config, [
            'uri' => 'required',
            'key' => 'required'
        ], $create);

        if ($validator->fails()) {
            $messages = $validator->messages()->getMessages();
            throw new BadRequestException('Validation failed.', null, null, $messages);
        }

        return true;
    }

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