<?php
namespace DreamFactory\Core\Azure;

use DreamFactory\Core\Azure\Models\BlobConfig;
use DreamFactory\Core\Azure\Models\DocumentDbConfig;
use DreamFactory\Core\Azure\Models\TableConfig;
use DreamFactory\Core\Azure\Services\Blob;
use DreamFactory\Core\Azure\Services\DocumentDB;
use DreamFactory\Core\Azure\Services\Table;
use DreamFactory\Core\Enums\ServiceTypeGroups;
use DreamFactory\Core\Services\ServiceManager;
use DreamFactory\Core\Services\ServiceType;

class ServiceProvider extends \Illuminate\Support\ServiceProvider
{
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
                    'config_handler'  => BlobConfig::class,
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
                    'config_handler'  => TableConfig::class,
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
