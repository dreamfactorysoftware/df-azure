<?php
namespace DreamFactory\Core\Azure\Services;

use DreamFactory\Core\Services\RemoteFileService;
use DreamFactory\Core\Azure\Components\AzureBlobFileSystem;

/**
 * Class Blob
 *
 * @package DreamFactory\Core\Azure\Services
 */
class Blob extends RemoteFileService
{
    /**
     * {@inheritdoc}
     */
    public function setDriver($config)
    {
        $this->driver = new AzureBlobFileSystem($config);
    }
}