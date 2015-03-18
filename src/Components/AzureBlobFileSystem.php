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

namespace DreamFactory\Rave\Azure\Components;

use InvalidArgumentException;
use DreamFactory\Rave\Components\RemoteFileSystem;
use DreamFactory\Library\Utility\ArrayUtils;
use DreamFactory\Rave\Exceptions\RaveException;
use DreamFactory\Rave\Exceptions\NotFoundException;
use DreamFactory\Rave\Exceptions\InternalServerErrorException;
use WindowsAzure\Blob\BlobRestProxy;
use WindowsAzure\Common\ServicesBuilder;
use WindowsAzure\Blob\Models\GetBlobResult;
use WindowsAzure\Blob\Models\ListBlobsResult;
use WindowsAzure\Blob\Models\ListBlobsOptions;
use WindowsAzure\Blob\Models\CreateBlobOptions;
use WindowsAzure\Blob\Models\CreateContainerOptions;
use WindowsAzure\Blob\Models\GetBlobPropertiesResult;

/**
 * Class AzureBlobFileSystem
 *
 * @package DreamFactory\Rave\Azure\Components
 */
class AzureBlobFileSystem extends RemoteFileSystem
{
    //*************************************************************************
    //	Members
    //*************************************************************************

    /**
     * @var BlobRestProxy|null
     */
    protected $_blobConn = null;

    //*************************************************************************
    //	Methods
    //*************************************************************************

    /**
     * @throws RaveException
     */
    protected function checkConnection()
    {
        if ( !isset( $this->_blobConn ) )
        {
            throw new RaveException( 'No valid connection to blob file storage.' );
        }
    }

    /**
     * Connects to a Azure Blob Storage
     *
     * @param array $config Authentication configuration
     *
     * @throws InvalidArgumentException
     * @throws InternalServerErrorException
     * @throws Exception
     */
    public function __construct( $config )
    {
        $_credentials = $config;
        //Session::replaceLookups( $_credentials, true );

        $_connectionString = ArrayUtils::get( $_credentials, 'connection_string' );
        if ( empty( $_connectionString ) )
        {
            $_name = ArrayUtils::get( $_credentials, 'account_name', ArrayUtils::get( $_credentials, 'AccountName' ) );
            if ( empty( $_name ) )
            {
                throw new InvalidArgumentException( 'WindowsAzure account name can not be empty.' );
            }

            $_key = ArrayUtils::get( $_credentials, 'account_key', ArrayUtils::get( $_credentials, 'AccountKey' ) );
            if ( empty( $_key ) )
            {
                throw new InvalidArgumentException( 'WindowsAzure account key can not be empty.' );
            }

            $_protocol = ArrayUtils::get( $_credentials, 'protocol', 'https' );
            $_connectionString = "DefaultEndpointsProtocol=$_protocol;AccountName=$_name;AccountKey=$_key";
        }

        try
        {
            $this->_blobConn = ServicesBuilder::getInstance()->createBlobService( $_connectionString );
        }
        catch ( \Exception $_ex )
        {
            throw new InternalServerErrorException( "Windows Azure Blob Service Exception:\n{$_ex->getMessage()}" );
        }
    }

    /**
     * Object destructor
     */
    public function __destruct()
    {
        unset( $this->_blobConn );
    }

    /**
     * @param $name
     *
     * @return mixed
     */
    private function fixBlobName( $name )
    {
        // doesn't like spaces in the name, anything else?
        return str_replace( ' ', '%20', $name );
    }

    /**
     * List all containers, just names if noted
     *
     * @param bool $include_properties If true, additional properties are retrieved
     *
     * @throws \Exception
     * @return array
     */
    public function listContainers( $include_properties = false )
    {
        $this->checkConnection();

        /** @var \WindowsAzure\Blob\Models\ListContainersResult $result */
        $result = $this->_blobConn->listContainers();

        /** @var \WindowsAzure\Blob\Models\Container[] $_items */
        $_items = $result->getContainers();
        $result = array();
        foreach ( $_items as $_item )
        {
            $_name = $_item->getName();
            $out = array( 'name' => $_name, 'path' => $_name );
            if ( $include_properties )
            {
                $props = $_item->getProperties();
                $out['last_modified'] = gmdate( static::TIMESTAMP_FORMAT, $props->getLastModified()->getTimestamp() );
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
     * @param bool   $include_properties
     *
     * @return array
     */
    public function getContainer( $container, $include_files = true, $include_folders = true, $full_tree = false, $include_properties = false )
    {
        $this->checkConnection();

        $result = $this->getFolder( $container, '', $include_files, $include_folders, $full_tree, false );
        $result['name'] = $container;
        if ( $include_properties )
        {
            /** @var \WindowsAzure\Blob\Models\GetContainerPropertiesResult $props */
            $props = $this->_blobConn->getContainerProperties( $container );
            $result['last_modified'] = gmdate( static::TIMESTAMP_FORMAT, $props->getLastModified()->getTimestamp() );
        }

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
    public function containerExists( $container )
    {
        $this->checkConnection();
        try
        {
            $this->_blobConn->getContainerProperties( $container );

            return true;
        }
        catch ( \Exception $ex )
        {
            if ( false === stripos( $ex->getMessage(), 'does not exist' ) )
            {
                throw $ex;
            }
        }

        return false;
    }

    /**
     * @param array $properties
     * @param array $metadata
     *
     * @throws RaveException
     * @return array
     */
    public function createContainer( $properties, $metadata = array() )
    {
        $this->checkConnection();

        $_name = ArrayUtils::get( $properties, 'name', ArrayUtils::get( $properties, 'path' ) );
        if ( empty( $_name ) )
        {
            throw new RaveException( 'No name found for container in create request.' );
        }
        $options = new CreateContainerOptions();
        $options->setMetadata( $metadata );
//		$options->setPublicAccess('blob');

        $this->_blobConn->createContainer( $_name, $options );

        return array( 'name' => $_name, 'path' => $_name );
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
    public function updateContainerProperties( $container, $properties = array() )
    {
        $this->checkConnection();

        $options = new CreateContainerOptions();
        $options->setMetadata( $properties );
//		$options->setPublicAccess('blob');

        $this->_blobConn->setContainerMetadata( $container, $options );
    }

    /**
     * @param string $container
     * @param bool   $force
     *
     * @throws \Exception
     * @return void
     */
    public function deleteContainer( $container, $force = false )
    {
        try
        {
            $this->checkConnection();
            $this->_blobConn->deleteContainer( $container );
        }
        catch ( \Exception $ex )
        {
            if ( false === stripos( $ex->getMessage(), 'does not exist' ) )
            {
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
    public function blobExists( $container, $name )
    {
        try
        {
            $this->checkConnection();
            $name = $this->fixBlobName( $name );
            $this->_blobConn->getBlobProperties( $container, $name );

            return true;
        }
        catch ( \Exception $ex )
        {
            if ( false === stripos( $ex->getMessage(), 'does not exist' ) )
            {
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
    public function putBlobData( $container, $name, $blob = '', $type = '' )
    {
        $this->checkConnection();

        $options = new CreateBlobOptions();

        if ( !empty( $type ) )
        {
            $options->setContentType( $type );
        }

        $this->_blobConn->createBlockBlob( $container, $this->fixBlobName( $name ), $blob, $options );
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
    public function putBlobFromFile( $container, $name, $localFileName = '', $type = '' )
    {
        $_blob = file_get_contents( $localFileName );
        if ( false === $_blob )
        {
            throw new InternalServerErrorException( "Failed to get contents of uploaded file." );
        }

        $this->putBlobData( $container, $name, $_blob, $type );
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
    public function copyBlob( $container, $name, $src_container, $src_name, $properties = array() )
    {
        $this->checkConnection();
        $this->_blobConn->copyBlob( $container, $this->fixBlobName( $name ), $src_container, $this->fixBlobName( $src_name ) );
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
    public function getBlobAsFile( $container, $name, $localFileName = '' )
    {
        $this->checkConnection();
        /** @var GetBlobResult $results */
        $results = $this->_blobConn->getBlob( $container, $this->fixBlobName( $name ) );
        file_put_contents( $localFileName, stream_get_contents( $results->getContentStream() ) );
    }

    /**
     * @param string $container
     * @param string $name
     *
     * @return mixed|string
     * @return string
     */
    public function getBlobData( $container, $name )
    {
        $this->checkConnection();
        /** @var GetBlobResult $results */
        $results = $this->_blobConn->getBlob( $container, $this->fixBlobName( $name ) );

        return stream_get_contents( $results->getContentStream() );
    }

    /**
     * @param string $container
     * @param string $name
     *
     * @return void
     * @throws \Exception
     */
    public function deleteBlob( $container, $name )
    {
        try
        {
            $this->checkConnection();
            $this->_blobConn->deleteBlob( $container, $this->fixBlobName( $name ) );
        }
        catch ( \Exception $ex )
        {
            if ( false === stripos( $ex->getMessage(), 'does not exist' ) )
            {
                throw $ex;
            }
        }
    }

    /**
     * List blobs
     *
     * @param  string $container Container name
     * @param  string $prefix    Optional. Filters the results to return only blobs whose name begins with the specified prefix.
     * @param  string $delimiter Optional. Delimiter, i.e. '/', for specifying folder hierarchy
     *
     * @return array
     * @throws \Exception
     */
    public function listBlobs( $container, $prefix = '', $delimiter = '' )
    {
        $this->checkConnection();
        $options = new ListBlobsOptions();

        if ( !empty( $delimiter ) )
        {
            $options->setDelimiter( $delimiter );
        }

        if ( !empty( $prefix ) )
        {
            $options->setPrefix( $prefix );
        }

        /** @var ListBlobsResult $results */
        $results = $this->_blobConn->listBlobs( $container, $options );
        $blobs = $results->getBlobs();
        $prefixes = $results->getBlobPrefixes();
        $out = array();

        /** @var \WindowsAzure\Blob\Models\Blob $blob */
        foreach ( $blobs as $blob )
        {
            $name = $blob->getName();
            if ( 0 == strcmp( $prefix, $name ) )
            {
                continue;
            }
            $props = $blob->getProperties();
            $out[] = array(
                'name'             => $name,
                'last_modified'    => gmdate( static::TIMESTAMP_FORMAT, $props->getLastModified()->getTimestamp() ),
                'content_length'   => $props->getContentLength(),
                'content_type'     => $props->getContentType(),
                'content_encoding' => $props->getContentEncoding(),
                'content_language' => $props->getContentLanguage()
            );
        }

        foreach ( $prefixes as $blob )
        {
            $out[] = array(
                'name' => $blob->getName()
            );
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
    public function getBlobProperties( $container, $name )
    {
        $this->checkConnection();
        $name = $this->fixBlobName( $name );
        /** @var GetBlobPropertiesResult $result */
        $result = $this->_blobConn->getBlobProperties( $container, $name );
        $props = $result->getProperties();
        $file = array(
            'name'           => $name,
            'last_modified'  => gmdate( static::TIMESTAMP_FORMAT, $props->getLastModified()->getTimestamp() ),
            'content_length' => $props->getContentLength(),
            'content_type'   => $props->getContentType()
        );

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
    public function streamBlob( $container, $blobName, $params = array() )
    {
        try
        {
            $this->checkConnection();
            /** @var GetBlobResult $blob */
            $blob = $this->_blobConn->getBlob( $container, $blobName );
            $props = $blob->getProperties();

            header( 'Last-Modified: ' . gmdate( static::TIMESTAMP_FORMAT, $props->getLastModified()->getTimestamp() ) );
            header( 'Content-Type: ' . $props->getContentType() );
            header( 'Content-Transfer-Encoding: ' . $props->getContentEncoding() );
            header( 'Content-Length:' . $props->getContentLength() );

            $disposition = ( isset( $params['disposition'] ) && !empty( $params['disposition'] ) ) ? $params['disposition'] : 'inline';

            header( "Content-Disposition: $disposition; filename=\"$blobName\";" );
            fpassthru( $blob->getContentStream() );
//            $this->_blobConn->registerStreamWrapper();
//            $blobUrl = 'azure://' . $container . '/' . $blobName;
//            readfile($blobUrl);
        }
        catch ( \Exception $ex )
        {
            if ( 'Resource could not be accessed.' == $ex->getMessage() )
            {
                $status_header = "HTTP/1.1 404 The specified file '$blobName' does not exist.";
                header( $status_header );
                header( 'Content-Type: text/html' );
            }
            else
            {
                throw $ex;
            }
        }
    }
}