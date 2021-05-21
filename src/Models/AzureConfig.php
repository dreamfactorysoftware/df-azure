<?php
namespace DreamFactory\Core\Azure\Models;

use DreamFactory\Core\Models\BaseServiceConfigModel;

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

    protected $rules = [
        'account_name' => 'required',
        'account_key'  => 'required',
        'protocol'     => 'required'
    ];

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
                $schema['display'] = 'true';
                break;
            case 'account_name':
                $schema['description'] = 'A Windows Azure storage account name.';
                $schema['display'] = 'true';
                break;
            case 'account_key':
                $schema['description'] = 'A Windows Azure storage account key.';
                $schema['display'] = 'true';
                break;
        }
    }

}