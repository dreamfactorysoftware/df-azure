<?php
namespace DreamFactory\Core\Azure;

use DreamFactory\Core\Azure\Components\AzureBlobConfig;
use DreamFactory\Core\Azure\Models\AzureConfig;
use DreamFactory\Core\Azure\Models\DocumentDbConfig;
use DreamFactory\Core\Azure\Services\Blob;
use DreamFactory\Core\Azure\Services\DocumentDB;
use DreamFactory\Core\Azure\Services\Table;
use DreamFactory\Core\Components\ServiceDocBuilder;
use DreamFactory\Core\Enums\ServiceTypeGroups;
use DreamFactory\Core\Services\ServiceManager;
use DreamFactory\Core\Services\ServiceType;

class ServiceProvider extends \Illuminate\Support\ServiceProvider
{
    use ServiceDocBuilder;

    public function register()
    {
        // Add our service types.
        $this->app->resolving('df.service', function (ServiceManager $df) {
            $df->addType(
                new ServiceType([
                    'name'            => 'azure_blob',
                    'label'           => 'Azure Blob Storage',
                    'description'     => 'File service supporting the Microsoft Azure Blob storage.',
                    'group'           => ServiceTypeGroups::FILE,
                    'config_handler'  => AzureBlobConfig::class,
                    'default_api_doc' => function ($service) {
                        return $this->buildServiceDoc($service->id, Blob::getApiDocInfo($service));
                    },
                    'factory'         => function ($config) {
                        return new Blob($config);
                    }
                ])
            );
            $df->addType(
                new ServiceType([
                    'name'            => 'azure_table',
                    'label'           => 'Azure Table Storage',
                    'description'     => 'Database service supporting the Microsoft Azure Table storage.',
                    'group'           => ServiceTypeGroups::DATABASE,
                    'config_handler'  => AzureConfig::class,
                    'default_api_doc' => function ($service) {
                        return $this->buildServiceDoc($service->id, Table::getApiDocInfo($service));
                    },
                    'factory'         => function ($config) {
                        return new Table($config);
                    }
                ])
            );
            $df->addType(
                new ServiceType([
                    'name'            => 'azure_documentdb',
                    'label'           => 'Azure DocumentDB',
                    'description'     => 'Database service supporting the Microsoft Azure DocumentDB.',
                    'group'           => ServiceTypeGroups::DATABASE,
                    'config_handler'  => DocumentDbConfig::class,
                    'default_api_doc' => function ($service) {
                        return $this->buildServiceDoc($service->id, DocumentDB::getApiDocInfo($service));
                    },
                    'factory'         => function ($config) {
                        return new DocumentDB($config);
                    }
                ])
            );
        });
    }

    public function boot()
    {
        // add migrations
        $this->loadMigrationsFrom(__DIR__ . '/../database/migrations');
    }
}
