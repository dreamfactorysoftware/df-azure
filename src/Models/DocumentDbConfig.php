<?php

namespace DreamFactory\Core\Azure\Models;

use DreamFactory\Core\Models\BaseServiceConfigModel;
use DreamFactory\Core\Exceptions\BadRequestException;

class DocumentDbConfig extends BaseServiceConfigModel
{
    /** @var string  */
    protected $table = 'documentdb_config';

    /** @var array  */
    protected $fillable = ['service_id', 'uri', 'key', 'database'];

    /** @var array  */
    protected $encrypted = ['key'];

    /** @var array  */
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