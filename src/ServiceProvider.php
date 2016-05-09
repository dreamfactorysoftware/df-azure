<?php
namespace DreamFactory\Core\Azure;

use DreamFactory\Core\Azure\Components\AzureBlobConfig;
use DreamFactory\Core\Azure\Models\AzureConfig;
use DreamFactory\Core\Azure\Services\Blob;
use DreamFactory\Core\Azure\Services\Table;
use DreamFactory\Core\Enums\ServiceTypeGroups;
use DreamFactory\Core\Services\ServiceManager;
use DreamFactory\Core\Services\ServiceType;

class ServiceProvider extends \Illuminate\Support\ServiceProvider
{
    public function register()
    {
        // Add our service types.
        $this->app->resolving('df.service', function (ServiceManager $df){
            $df->addType(
                new ServiceType([
                    'name'           => 'azure_blob',
                    'label'          => 'Azure Blob Storage',
                    'description'    => 'File service supporting the Microsoft Azure Blob storage.',
                    'group'          => ServiceTypeGroups::FILE,
                    'config_handler' => AzureBlobConfig::class,
                    'factory'          => function ($config){
                        return new Blob($config);
                    }
                ])
            );
            $df->addType(
                new ServiceType([
                    'name'           => 'azure_table',
                    'label'          => 'Azure Table Storage',
                    'description'    => 'Database service supporting the Microsoft Azure Table storage.',
                    'group'          => ServiceTypeGroups::DATABASE,
                    'config_handler' => AzureConfig::class,
                    'make'           => function ($config){
                        return new Table($config);
                    }
                ])
            );
        });
    }
}
