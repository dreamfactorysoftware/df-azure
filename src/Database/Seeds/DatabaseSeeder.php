<?php
namespace DreamFactory\Core\Azure\Database\Seeds;

use DreamFactory\Core\Database\Seeds\BaseModelSeeder;

class DatabaseSeeder extends BaseModelSeeder
{
    protected $modelClass = 'DreamFactory\\Core\\Models\\ServiceType';

    protected $records = [
        [
            'name'           => 'azure_blob',
            'class_name'     => "DreamFactory\\Core\\Azure\\Services\\Blob",
            'config_handler' => "DreamFactory\\Core\\Azure\\Models\\AzureConfig",
            'label'          => 'Azure Blob Storage',
            'description'    => 'File service supporting the Microsoft Azure Blob Storage.',
            'group'          => 'files',
            'singleton'      => 1
        ],
        [
            'name'           => 'azure_table',
            'class_name'     => "DreamFactory\\Core\\Azure\\Services\\Table",
            'config_handler' => "DreamFactory\\Core\\Azure\\Models\\AzureConfig",
            'label'          => 'Azure Table Storage',
            'description'    => 'NoSql database service supporting the Microsoft Azure storage system.',
            'group'          => 'NoSql Databases',
            'singleton'      => 1
        ]
    ];
}