<?php
namespace DreamFactory\Core\Azure\Database\Seeds;

use DreamFactory\Core\Azure\Components\AzureBlobConfig;
use DreamFactory\Core\Azure\Models\AzureConfig;
use DreamFactory\Core\Azure\Services\Blob;
use DreamFactory\Core\Azure\Services\Table;
use DreamFactory\Core\Database\Seeds\BaseModelSeeder;
use DreamFactory\Core\Enums\ServiceTypeGroups;
use DreamFactory\Core\Models\ServiceType;

class DatabaseSeeder extends BaseModelSeeder
{
    protected $modelClass = ServiceType::class;

    protected $records = [
        [
            'name'           => 'azure_blob',
            'class_name'     => Blob::class,
            'config_handler' => AzureBlobConfig::class,
            'label'          => 'Azure Blob Storage',
            'description'    => 'File service supporting the Microsoft Azure Blob storage.',
            'group'          => ServiceTypeGroups::FILE,
            'singleton'      => false
        ],
        [
            'name'           => 'azure_table',
            'class_name'     => Table::class,
            'config_handler' => AzureConfig::class,
            'label'          => 'Azure Table Storage',
            'description'    => 'Database service supporting the Microsoft Azure Table storage.',
            'group'          => ServiceTypeGroups::DATABASE,
            'singleton'      => false
        ]
    ];
}