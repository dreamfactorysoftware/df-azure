<?php

class AzureDbConfigTest extends \DreamFactory\Core\Database\Testing\DbServiceConfigTestCase
{
    protected $types = ['azure_table', 'azure_documentdb'];

    public function getDbServiceConfig($name, $type, $maxRecords = null)
    {
        $config = [
            'name' => $name,
            'label' => 'test db service',
            'type' => $type,
            'is_active' => true,
            'config' => [
                'account_name' => 'my-account',
                'account_key' => 'my-key',
                'protocol' => 'my-protocol',
                'uri' => 'my-uri',
                'key' => 'my-key',
                'database' => 'my-db'
            ]
        ];

        if(!empty($maxRecords)){
            $config['config']['max_records'] = $maxRecords;
        }

        return $config;
    }
}