<?php

namespace DreamFactory\Core\Azure\Components;

use DreamFactory\Core\Exceptions\RestException;
use DreamFactory\DocumentDb\Client;
use DreamFactory\DocumentDb\Resources\Collection;
use DreamFactory\DocumentDb\Resources\Document;

class DocumentDBConnection
{
    /** @var Client */
    protected $client;

    /** @var string */
    protected $database;

    /** @var array */
    protected $headers = [];

    /** Limit option */
    const OPT_LIMIT = 'limit';

    /**
     * DocumentDBConnection constructor.
     *
     * @param $uri
     * @param $key
     * @param $database
     */
    public function __construct($uri, $key, $database)
    {
        $this->client = new Client($uri, $key);
        $this->database = $database;
    }

    /**
     * Set additional request headers.
     *
     * @param array $header DocumentDB REST API request header
     */
    public function setHeaders(array $header)
    {
        $this->headers = array_merge($this->headers, $header);
    }

    /**
     * Sets options.
     *
     * @param array $options
     */
    public function setOptions(array $options)
    {
        $limit = array_get($options, static::OPT_LIMIT);

        if (!empty($limit)) {
            $this->setHeaders(['x-ms-max-item-count' => $limit]);
        }
    }

    /**
     * Sets request headers.
     *
     * @return array
     */
    public function getHeaders()
    {
        $headers = [];
        foreach ($this->headers as $key => $value) {
            $headers[] = $key . ': ' . $value;
        }

        return $headers;
    }

    /*********************************
     * Collection operations
     *********************************/

    /**
     * List all collections in a Database.
     *
     * @param array $options Operation options
     *
     * @return array
     */
    public function listCollections(array $options = [])
    {
        $this->setOptions($options);
        $coll = new Collection($this->client, $this->database);
        $coll->setHeaders($this->getHeaders());
        $rs = $coll->getAll();
        $this->checkResponse($rs);
        $colls = array_get($rs, 'DocumentCollections');
        $list = [];
        if (!empty($colls)) {
            foreach ($colls as $coll) {
                $list[] = array_get($coll, 'id');
            }
        }

        return $list;
    }

    /**
     * Retrieves a collection information.
     *
     * @param string $id      Collection ID
     * @param array  $options Operation options
     *
     * @return array
     */
    public function getCollection($id, array $options = [])
    {
        $this->setOptions($options);
        $coll = new Collection($this->client, $this->database);
        $coll->setHeaders($this->getHeaders());
        $rs = $coll->get($id);
        $this->checkResponse($rs);
        unset($rs['_curl_info']);

        return $rs;
    }

    /**
     * Creates a collection.
     *
     * @param array $data    Collection data
     * @param array $options Operation options
     *
     * @return array
     */
    public function createCollection(array $data, array $options = [])
    {
        $this->setOptions($options);
        $coll = new Collection($this->client, $this->database);
        $coll->setHeaders($this->getHeaders());
        $rs = $coll->create($data);
        $this->checkResponse($rs);
        unset($rs['_curl_info']);

        return $rs;
    }

    /**
     * Replaces a collection.
     *
     * @param array  $data    Collection data
     * @param string $id
     * @param array  $options Operation options
     *
     * @return array
     */
    public function replaceCollection(array $data, $id, array $options = [])
    {
        $this->setOptions($options);
        $coll = new Collection($this->client, $this->database);
        $coll->setHeaders($this->getHeaders());
        $rs = $coll->replace($data, $id);
        $this->checkResponse($rs);
        unset($rs['_curl_info']);

        return $rs;
    }

    /**
     * Deletes a collection.
     *
     * @param string $id      Collection ID
     * @param array  $options Operation options
     *
     * @return array
     */
    public function deleteCollection($id, array $options = [])
    {
        $this->setOptions($options);
        $coll = new Collection($this->client, $this->database);
        $coll->setHeaders($this->getHeaders());
        $rs = $coll->delete($id);
        $this->checkResponse($rs);
        unset($rs['_curl_info']);

        return $rs;
    }

    /*********************************
     * Document operations
     *********************************/

    /**
     * List all documents in a collection.
     *
     * @param string $collection Collection to list documents from
     * @param array  $options    Operation options
     *
     * @return array
     */
    public function listDocuments($collection, array $options = [])
    {
        $this->setOptions($options);
        $doc = new Document($this->client, $this->database, $collection);
        $doc->setHeaders($this->getHeaders());
        $rs = $doc->getAll();
        $this->checkResponse($rs);
        unset($rs['_curl_info']);

        return $rs;
    }

    /**
     * Retrieves a document.
     *
     * @param string $collection Collection to list document from
     * @param string $id         ID of the document to retrieve
     * @param array  $options    Operation options
     *
     * @return array
     */
    public function getDocument($collection, $id, array $options = [])
    {
        $this->setOptions($options);
        $doc = new Document($this->client, $this->database, $collection);
        $doc->setHeaders($this->getHeaders());
        $rs = $doc->get($id);
        $this->checkResponse($rs);
        unset($rs['_curl_info']);

        return $rs;
    }

    /**
     * Creates a document.
     *
     * @param string $collection Collection to create a document in
     * @param array  $data       Document data
     * @param array  $options    Operation options
     *
     * @return array
     */
    public function createDocument($collection, array $data, array $options = [])
    {
        $this->setOptions($options);
        $doc = new Document($this->client, $this->database, $collection);
        $doc->setHeaders($this->getHeaders());
        $rs = $doc->create($data);
        $this->checkResponse($rs);
        unset($rs['_curl_info']);

        return $rs;
    }

    /**
     * Replaces a document.
     *
     * @param string $collection Collection to replace a document in
     * @param array  $data       Document data
     * @param string $id         ID of the document being replaced
     * @param array  $options    Operation options
     *
     * @return array
     */
    public function replaceDocument($collection, array $data, $id, array $options = [])
    {
        $this->setOptions($options);
        $doc = new Document($this->client, $this->database, $collection);
        $doc->setHeaders($this->getHeaders());
        $rs = $doc->replace($data, $id);
        $this->checkResponse($rs);
        unset($rs['_curl_info']);

        return $rs;
    }

    /**
     * Deletes a document.
     *
     * @param string $collection Collection to delete a document from
     * @param string $id         ID of the document to delete
     * @param array  $options    Operation options
     *
     * @return array
     */
    public function deleteDocument($collection, $id, array $options = [])
    {
        $this->setOptions($options);
        $doc = new Document($this->client, $this->database, $collection);
        $doc->setHeaders($this->getHeaders());
        $rs = $doc->delete($id);
        $this->checkResponse($rs);
        unset($rs['_curl_info']);

        return $rs;
    }

    /**
     * Queries a collection for documents.
     *
     * @param string $collection Collection to perform the query on
     * @param string $sql        SQL query string
     * @param array  $params     Query parameters
     * @param array  $options    Operation options
     *
     * @return array
     */
    public function queryDocument($collection, $sql, array $params = [], array $options = [])
    {
        $this->setOptions($options);
        $doc = new Document($this->client, $this->database, $collection);
        $doc->setHeaders($this->getHeaders());
        $rs = $doc->query($sql, $params);
        $this->checkResponse($rs);
        unset($rs['_curl_info']);

        return $rs;
    }

    /**
     * Checks response status code for exceptions.
     *
     * @param array $response
     *
     * @throws \DreamFactory\Core\Exceptions\RestException
     */
    protected function checkResponse(array $response)
    {
        $responseCode = intval(array_get($response, '_curl_info.http_code'));
        $message = array_get($response, 'message');

        if ($responseCode >= 400) {
            $context = ['response_headers' => array_get($response, '_curl_info.response_headers')];
            throw new RestException($responseCode, $message, null, null, $context);
        }
    }
}