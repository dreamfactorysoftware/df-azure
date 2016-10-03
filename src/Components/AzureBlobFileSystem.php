<?php
namespace DreamFactory\Core\Azure\Components;

use DreamFactory\Core\Utility\Session;
use InvalidArgumentException;
use DreamFactory\Core\Components\RemoteFileSystem;
use DreamFactory\Core\Exceptions\DfException;
use DreamFactory\Core\Exceptions\NotFoundException;
use DreamFactory\Core\Exceptions\InternalServerErrorException;
use MicrosoftAzure\Storage\Blob\BlobRestProxy;
use MicrosoftAzure\Storage\Blob\Models\GetBlobOptions;
use MicrosoftAzure\Storage\Common\ServicesBuilder;
use MicrosoftAzure\Storage\Blob\Models\GetBlobResult;
use MicrosoftAzure\Storage\Blob\Models\ListBlobsResult;
use MicrosoftAzure\Storage\Blob\Models\ListBlobsOptions;
use MicrosoftAzure\Storage\Blob\Models\CreateBlobOptions;
use MicrosoftAzure\Storage\Blob\Models\CreateContainerOptions;
use MicrosoftAzure\Storage\Blob\Models\GetBlobPropertiesResult;

/**
 * Class AzureBlobFileSystem
 *
 * @package DreamFactory\Core\Azure\Components
 */
class AzureBlobFileSystem extends RemoteFileSystem
{
    //*************************************************************************
    //	Members
    //*************************************************************************

    /**
     * @var BlobRestProxy|null
     */
    protected $blobConn = null;

    //*************************************************************************
    //	Methods
    //*************************************************************************

    /**
     * @throws DfException
     */
    protected function checkConnection()
    {
        if (!isset($this->blobConn)) {
            throw new DfException('No valid connection to blob file storage.');
        }
    }

    /**
     * Connects to a Azure Blob Storage
     *
     * @param array $config Authentication configuration
     *
     * @throws InvalidArgumentException
     * @throws InternalServerErrorException
     * @throws \Exception
     */
    public function __construct($config)
    {
        $credentials = $config;
        $this->container = array_get($config, 'container');

        Session::replaceLookups($credentials, true);

        $connectionString = array_get($credentials, 'connection_string');
        if (empty($connectionString)) {
            $name = array_get($credentials, 'account_name', array_get($credentials, 'AccountName'));
            if (empty($name)) {
                throw new InvalidArgumentException('WindowsAzure account name can not be empty.');
            }

            $key = array_get($credentials, 'account_key', array_get($credentials, 'AccountKey'));
            if (empty($key)) {
                throw new InvalidArgumentException('WindowsAzure account key can not be empty.');
            }

            $protocol = array_get($credentials, 'protocol', 'https');
            $connectionString = "DefaultEndpointsProtocol=$protocol;AccountName=$name;AccountKey=$key";
        }

        try {
            $this->blobConn = ServicesBuilder::getInstance()->createBlobService($connectionString);

            if (!$this->containerExists($this->container)) {
                $this->createContainer(['name' => $this->container]);
            }
        } catch (\Exception $ex) {
            throw new InternalServerErrorException("Windows Azure Blob Service Exception:\n{$ex->getMessage()}");
        }
    }

    /**
     * Object destructor
     */
    public function __destruct()
    {
        unset($this->blobConn);
    }

    /**
     * @param $name
     *
     * @return mixed
     */
    private function fixBlobName($name)
    {
        // doesn't like spaces in the name, anything else?
        return str_replace(' ', '%20', $name);
    }

    /**
     * List all containers, just names if noted
     *
     * @param bool $include_properties If true, additional properties are retrieved
     *
     * @throws \Exception
     * @return array
     */
    public function listContainers($include_properties = false)
    {
        $this->checkConnection();

        if (!empty($this->container)) {
            return $this->listResource($include_properties);
        }

        /** @var \MicrosoftAzure\Storage\Blob\Models\ListContainersResult $result */
        $result = $this->blobConn->listContainers();

        /** @var \MicrosoftAzure\Storage\Blob\Models\Container[] $items */
        $items = $result->getContainers();
        $result = [];
        foreach ($items as $item) {
            $name = $item->getName();
            $out = ['name' => $name, 'path' => $name];
            if ($include_properties) {
                $props = $item->getProperties();
                $out['last_modified'] = gmdate(static::TIMESTAMP_FORMAT, $props->getLastModified()->getTimestamp());
            }
            $result[] = $out;
        }

        return $result;
    }

    /**
     * Gets all properties of a particular container, if options are false,
     * otherwise include content from the container
     *
     * @param string $container Container name
     * @param bool   $include_files
     * @param bool   $include_folders
     * @param bool   $full_tree
     *
     * @return array
     */
    public function getContainer($container, $include_files = true, $include_folders = true, $full_tree = false)
    {
        $this->checkConnection();

        return $this->getFolder($container, '', $include_files, $include_folders, $full_tree);
    }

    public function getContainerProperties($container)
    {
        $this->checkConnection();

        $result = ['name' => $container];
        /** @var \MicrosoftAzure\Storage\Blob\Models\GetContainerPropertiesResult $props */
        $props = $this->blobConn->getContainerProperties($container);
        $result['last_modified'] = gmdate(static::TIMESTAMP_FORMAT, $props->getLastModified()->getTimestamp());

        return $result;
    }

    /**
     * Check if a container exists
     *
     * @param  string $container Container name
     *
     * @return boolean
     * @throws \Exception
     */
    public function containerExists($container)
    {
        $this->checkConnection();
        try {
            $this->blobConn->getContainerProperties($container);

            return true;
        } catch (\Exception $ex) {
            if (false === stripos($ex->getMessage(), 'does not exist')) {
                throw $ex;
            }
        }

        return false;
    }

    /**
     * @param array $properties
     * @param array $metadata
     *
     * @throws DfException
     * @return array
     */
    public function createContainer($properties, $metadata = [])
    {
        $this->checkConnection();

        $name = array_get($properties, 'name', array_get($properties, 'path'));
        if (empty($name)) {
            throw new DfException('No name found for container in create request.');
        }
        $options = new CreateContainerOptions();
        $options->setMetadata($metadata);
//		$options->setPublicAccess('blob');

        $this->blobConn->createContainer($name, $options);

        return ['name' => $name, 'path' => $name];
    }

    /**
     * Update a container with some properties
     *
     * @param string $container
     * @param array  $properties
     *
     * @throws NotFoundException
     * @return void
     */
    public function updateContainerProperties($container, $properties = [])
    {
        $this->checkConnection();

        $options = new CreateContainerOptions();
        $options->setMetadata($properties);
//		$options->setPublicAccess('blob');

        $this->blobConn->setContainerMetadata($container, $options);
    }

    /**
     * @param string $container
     * @param bool   $force
     *
     * @throws \Exception
     * @return void
     */
    public function deleteContainer($container, $force = false)
    {
        try {
            $this->checkConnection();
            $this->blobConn->deleteContainer($container);
        } catch (\Exception $ex) {
            if (false === stripos($ex->getMessage(), 'does not exist')) {
                throw $ex;
            }
        }
    }

    /**
     * Check if a blob exists
     *
     * @param  string $container Container name
     * @param  string $name      Blob name
     *
     * @return boolean
     * @throws \Exception
     */
    public function blobExists($container, $name)
    {
        try {
            $this->checkConnection();
            $name = $this->fixBlobName($name);
            $this->blobConn->getBlobProperties($container, $name);

            return true;
        } catch (\Exception $ex) {
            if (false === stripos($ex->getMessage(), 'does not exist')) {
                throw $ex;
            }
        }

        return false;
    }

    /**
     * @param string $container
     * @param string $name
     * @param string $blob
     * @param string $type
     *
     * @return void
     */
    public function putBlobData($container, $name, $blob = '', $type = '')
    {
        $this->checkConnection();

        $options = new CreateBlobOptions();

        if (!empty($type)) {
            $options->setContentType($type);
        }

        $this->blobConn->createBlockBlob($container, $this->fixBlobName($name), $blob, $options);
    }

    /**
     * @param string $container
     * @param string $name
     * @param string $localFileName
     * @param string $type
     *
     * @throws InternalServerErrorException
     * @return void
     */
    public function putBlobFromFile($container, $name, $localFileName = '', $type = '')
    {
        $blob = file_get_contents($localFileName);
        if (false === $blob) {
            throw new InternalServerErrorException("Failed to get contents of uploaded file.");
        }

        $this->putBlobData($container, $name, $blob, $type);
    }

    /**
     * @param string $container
     * @param string $name
     * @param string $src_container
     * @param string $src_name
     *
     * @param array  $properties
     *
     * @return void
     */
    public function copyBlob($container, $name, $src_container, $src_name, $properties = [])
    {
        $this->checkConnection();
        $this->blobConn->copyBlob($container, $this->fixBlobName($name), $src_container,
            $this->fixBlobName($src_name));
    }

    /**
     * Get blob
     *
     * @param  string $container     Container name
     * @param  string $name          Blob name
     * @param  string $localFileName Local file name to store downloaded blob
     *
     * @return void
     */
    public function getBlobAsFile($container, $name, $localFileName = '')
    {
        $this->checkConnection();
        /** @var GetBlobResult $results */
        $results = $this->blobConn->getBlob($container, $this->fixBlobName($name));
        file_put_contents($localFileName, stream_get_contents($results->getContentStream()));
    }

    /**
     * @param string $container
     * @param string $name
     *
     * @return mixed|string
     * @return string
     */
    public function getBlobData($container, $name)
    {
        $this->checkConnection();
        /** @var GetBlobResult $results */
        $results = $this->blobConn->getBlob($container, $this->fixBlobName($name));

        return stream_get_contents($results->getContentStream());
    }

    /**
     * @param string $container
     * @param string $name
     *
     * @return void
     * @throws \Exception
     */
    public function deleteBlob($container, $name)
    {
        try {
            $this->checkConnection();
            $this->blobConn->deleteBlob($container, $this->fixBlobName($name));
        } catch (\Exception $ex) {
            if (false === stripos($ex->getMessage(), 'does not exist')) {
                throw $ex;
            }
        }
    }

    /**
     * List blobs
     *
     * @param  string $container Container name
     * @param  string $prefix    Optional. Filters the results to return only blobs whose name begins with the
     *                           specified prefix.
     * @param  string $delimiter Optional. Delimiter, i.e. '/', for specifying folder hierarchy
     *
     * @return array
     * @throws \Exception
     */
    public function listBlobs($container, $prefix = '', $delimiter = '')
    {
        $this->checkConnection();
        $options = new ListBlobsOptions();

        if (!empty($delimiter)) {
            $options->setDelimiter($delimiter);
        }

        if (!empty($prefix)) {
            $options->setPrefix($prefix);
        }

        /** @var ListBlobsResult $results */
        $results = $this->blobConn->listBlobs($container, $options);
        $blobs = $results->getBlobs();
        $prefixes = $results->getBlobPrefixes();
        $out = [];

        /** @var \MicrosoftAzure\Storage\Blob\Models\Blob $blob */
        foreach ($blobs as $blob) {
            $name = $blob->getName();
            if (0 == strcmp($prefix, $name)) {
                continue;
            }
            $props = $blob->getProperties();
            $out[] = [
                'name'             => $name,
                'last_modified'    => gmdate(static::TIMESTAMP_FORMAT, $props->getLastModified()->getTimestamp()),
                'content_length'   => $props->getContentLength(),
                'content_type'     => $props->getContentType(),
                'content_encoding' => $props->getContentEncoding(),
                'content_language' => $props->getContentLanguage()
            ];
        }

        foreach ($prefixes as $blob) {
            $out[] = [
                'name'           => $blob->getName(),
                'content_type'   => null,
                'content_length' => 0,
                'last_modified'  => null
            ];
        }

        return $out;
    }

    /**
     * List blob
     *
     * @param  string $container Container name
     * @param  string $name      Blob name
     *
     * @return array instance
     * @throws \Exception
     */
    public function getBlobProperties($container, $name)
    {
        $this->checkConnection();
        $name = $this->fixBlobName($name);
        /** @var GetBlobPropertiesResult $result */
        $result = $this->blobConn->getBlobProperties($container, $name);
        $props = $result->getProperties();
        $file = [
            'name'           => $name,
            'last_modified'  => gmdate(static::TIMESTAMP_FORMAT, $props->getLastModified()->getTimestamp()),
            'content_length' => $props->getContentLength(),
            'content_type'   => $props->getContentType()
        ];

        return $file;
    }

    /**
     * @param string $container
     * @param string $blobName
     * @param array  $params
     *
     * @throws \Exception
     * @return void
     */
    public function streamBlob($container, $blobName, $params = [])
    {
        try {
            $this->checkConnection();
            ///** @var GetBlobResult $blob */
            //$blob = $this->blobConn->getBlob($container, $blobName);
            $props = $this->blobConn->getBlobProperties($container, $blobName)->getProperties();
            $size = $props->getContentLength();
            $chunk = \Config::get('df.file_chunk_size');
            $index = 0;

            header('Last-Modified: ' . gmdate(static::TIMESTAMP_FORMAT, $props->getLastModified()->getTimestamp()));
            header('Content-Type: ' . $props->getContentType());
            header('Content-Transfer-Encoding: ' . $props->getContentEncoding());
            header('Content-Length:' . $size);

            $disposition =
                (isset($params['disposition']) && !empty($params['disposition'])) ? $params['disposition'] : 'inline';

            header("Content-Disposition: $disposition; filename=\"$blobName\";");
            ob_clean();

            while ($index < $size) {
                $option = new GetBlobOptions();
                $option->setRangeStart($index);
                $option->setRangeEnd($index + $chunk - 1);
                $blob = $this->blobConn->getBlob($container, $blobName, $option);
                $stream = $blob->getContentStream();
                $length = $blob->getProperties()->getContentLength();
                $index += $length;
                flush();
                fpassthru($stream);
            }
        } catch (\Exception $ex) {
            if ('Resource could not be accessed.' == $ex->getMessage()) {
                $status_header = "HTTP/1.1 404 The specified file '$blobName' does not exist.";
                header($status_header);
                header('Content-Type: text/html');
            } else {
                throw $ex;
            }
        }
    }
}