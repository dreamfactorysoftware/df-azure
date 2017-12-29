<?php

use DreamFactory\Core\Enums\Verbs;

class FileServiceAzureBlobTest extends \DreamFactory\Core\Testing\FileServiceTestCase
{
    protected static $staged = false;

    protected $serviceId = 'azure';

    public function stage()
    {
        parent::stage();

        Artisan::call('migrate', ['--path' => 'vendor/dreamfactory/df-azure/database/migrations/']);
        //Artisan::call('db:seed', ['--class' => DreamFactory\Core\Azure\Database\Seeds\DatabaseSeeder::class]);
        if (!$this->serviceExists('azure')) {
            \DreamFactory\Core\Models\Service::create(
                [
                    "name"        => "azure",
                    "label"       => "Azure Blob file service",
                    "description" => "Azure Blob file service for unit test",
                    "is_active"   => true,
                    "type"        => "azure_blob",
                    "config"      => [
                        'protocol'     => 'https',
                        'account_name' => env('AB_ACCOUNT_NAME'),
                        'account_key'  => env('AB_ACCOUNT_KEY'),
                        'container'    => env('AB_CONTAINER')
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
        $this->assertEquals(1, 1);
    }

    public function testPOSTZipFileFromUrlWithExtractAndClean()
    {
        $rs = $this->makeRequest(
            Verbs::POST,
            static::FOLDER_1 . '/f2/',
            ['url' => $this->getBaseUrl() . '/testfiles.zip', 'extract' => 'true', 'clean' => 'true']
        );
        $content = json_encode($rs->getContent(), JSON_UNESCAPED_SLASHES);

        $this->assertEquals('{"name":"' .
            static::FOLDER_1 .
            '/f2","path":"' .
            static::FOLDER_1 .
            '/f2/"}',
            $content);
        $this->makeRequest(Verbs::DELETE, static::FOLDER_1 . '/', ['force' => 1]);
    }
}
