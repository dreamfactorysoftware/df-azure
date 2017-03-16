<?php
namespace DreamFactory\Core\Azure\Services;

use DreamFactory\Core\File\Services\RemoteFileService;
use DreamFactory\Core\Azure\Components\AzureBlobFileSystem;
use DreamFactory\Core\Exceptions\InternalServerErrorException;

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
        $this->container = array_get($config, 'container');

        if (empty($this->container)) {
            throw new InternalServerErrorException('Azure blob container not specified. Please check configuration for file service - ' .
                $this->name);
        }
        $this->driver = new AzureBlobFileSystem($config);
    }
}