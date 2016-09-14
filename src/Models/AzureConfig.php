<?php
namespace DreamFactory\Core\Azure\Models;

use DreamFactory\Core\Models\BaseServiceConfigModel;
use DreamFactory\Core\Exceptions\BadRequestException;

/**
 * Class AzureConfig
 *
 * @package DreamFactory\Core\Azure\Models
 */
class AzureConfig extends BaseServiceConfigModel
{
    protected $table = 'azure_config';

    protected $fillable = ['service_id', 'account_name', 'account_key', 'protocol'];

    protected $encrypted = ['account_name', 'account_key'];

    protected $protected = ['account_key'];

    /**
     * {@inheritdoc}
     */
    public static function validateConfig($config, $create = true)
    {
        $validator = static::makeValidator($config, [
            'account_name' => 'required',
            'account_key'  => 'required',
            'protocol'     => 'required'
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
            case 'protocol':
                $schema['type'] = 'picklist';
                $schema['values'] = [
                    ['label' => 'HTTP', 'name' => 'http'],
                    ['label' => 'HTTPS', 'name' => 'https'],
                ];
                $schema['description'] = 'Select the HTTP protocol.';
                break;
            case 'account_name':
                $schema['description'] = 'A Windows Azure storage account name.';
                break;
            case 'account_key':
                $schema['description'] = 'A Windows Azure storage account key.';
                break;
        }
    }

}