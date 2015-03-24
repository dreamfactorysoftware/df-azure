<?php
/**
 * This file is part of the DreamFactory Rave(tm)
 *
 * DreamFactory Rave(tm) <http://github.com/dreamfactorysoftware/rave>
 * Copyright 2012-2014 DreamFactory Software, Inc. <support@dreamfactory.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace DreamFactory\Rave\Azure\Database\Seeds;

use Illuminate\Database\Seeder;
use DreamFactory\Rave\Models\ServiceType;

/**
 * Class DatabaseSeeder
 *
 * @package DreamFactory\Rave\Azure\Database\Seeds
 */
class DatabaseSeeder extends Seeder
{
    /**
     * Run the database seeds.
     *
     * @return void
     */
    public function run()
    {
        if ( !ServiceType::whereName( "azure_blob" )->count() )
        {
            // Add the service type
            ServiceType::create(
                [
                    'name'           => 'azure_blob',
                    'class_name'     => "DreamFactory\\Rave\\Azure\\Services\\Blob",
                    'config_handler' => "DreamFactory\\Rave\\Azure\\Models\\AzureConfig",
                    'label'          => 'Azure Blob Storage',
                    'description'    => 'File service supporting the Microsoft Azure Blob Storage.',
                    'group'          => 'files',
                    'singleton'      => 1
                ]
            );
            $this->command->info( 'Microsoft Azure Blob file service type seeded!' );
        }

        if ( !ServiceType::whereName( "azure_table" )->count() )
        {
            // Add the service type
            ServiceType::create(
                [
                    'name'           => 'azure_table',
                    'class_name'     => "DreamFactory\\Rave\\Azure\\Services\\Table",
                    'config_handler' => "DreamFactory\\Rave\\Azure\\Models\\AzureConfig",
                    'label'          => 'Azure Table Storage',
                    'description'    => 'NoSql database service supporting the Microsoft Azure storage system.',
                    'group'          => 'NoSql Databases',
                    'singleton'      => 1
                ]
            );
            $this->command->info( 'Microsoft Azure Table database service type seeded!' );
        }
    }
}