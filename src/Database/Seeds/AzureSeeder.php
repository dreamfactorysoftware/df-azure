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
 * Class AzureSeeder
 *
 * @package DreamFactory\Rave\Azure\Database\Seeds
 */
class AzureSeeder extends Seeder
{
    /**
     * Run the database seeds.
     *
     * @return void
     */
    public function run()
    {
        if ( !ServiceType::whereName( "azure_file" )->count() )
        {
            // Add the service type
            ServiceType::create(
                [
                    'name'           => 'azure_file',
                    'class_name'     => "DreamFactory\\Rave\\Azure\\Services\\AzureBlob",
                    'config_handler' => "DreamFactory\\Rave\\Azure\\Models\\AzureConfig",
                    'label'          => 'Azure file service',
                    'description'    => 'File service supporting the Microsoft Azure file system.',
                    'group'          => 'files',
                    'singleton'      => 1
                ]
            );
            $this->command->info( 'Microsoft AzureBlob file service type seeded!' );
        }
    }
}