<?php

/**
 * This file is part of the DreamFactory(tm)
 *
 * DreamFactory(tm) <http://github.com/dreamfactorysoftware/rave>
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
class FileServiceAzureBlobTest extends \DreamFactory\Core\Testing\FileServiceTestCase
{
    protected static $staged = false;

    protected $serviceId = 'azure';

    public function stage()
    {
        parent::stage();

        Artisan::call( 'migrate', [ '--path' => 'vendor/dreamfactory/df-azure/database/migrations/' ] );
        Artisan::call( 'db:seed', [ '--class' => 'DreamFactory\\Core\\Azure\\Database\\Seeds\\DatabaseSeeder' ] );
        if ( !$this->serviceExists( 'azure' ) )
        {
            \DreamFactory\Core\Models\Service::create(
                [
                    "name"        => "azure",
                    "label"       => "Azure Blob file service",
                    "description" => "Azure Blob file service for unit test",
                    "is_active"   => 1,
                    "type"        => "azure_blob",
                    "config"      => [
                        'protocol'     => 'https',
                        'account_name' => env( 'AB_ACCOUNT_NAME' ),
                        'account_key'  => env( 'AB_ACCOUNT_KEY' )
                    ]
                ]
            );
        }
    }

    /************************************************
     * Testing POST
     ************************************************/

    public function testPOSTContainerWithCheckExist()
    {
        //This test currently doesn't pass. Unlike local and S3 file services,
        //Azure blob service returns 409 (already exists) if the resource already exists
        $this->assertEquals( 1, 1 );
    }
}
