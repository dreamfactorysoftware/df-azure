<?php

namespace DreamFactory\Core\Azure\Components;

use DreamFactory\DocumentDb\Client;
use DreamFactory\DocumentDb\Resources\Collection;
use DreamFactory\DocumentDb\Resources\Document;

class DocumentDBConnection
{
    /** @var Client */
    protected $client;

    /** @var string */
    protected $database;

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

    /*********************************
     * Collection operations
     *********************************/

    /**
     * List all collections in a Database.
     *
     * @return array
     */
    public function listCollections()
    {
        $coll = new Collection($this->client, $this->database);
        $rs = $coll->list();
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
     * @param string $id Collection ID
     *
     * @return array
     */
    public function getCollection($id)
    {
        $coll = new Collection($this->client, $this->database);
        $rs = $coll->get($id);
        unset($rs['_curl_info']);

        return $rs;
    }

    /**
     * Creates a collection.
     *
     * @param array $data Collection data
     *
     * @return array
     */
    public function createCollection(array $data)
    {
        $coll = new Collection($this->client, $this->database);
        $rs = $coll->create($data);
        unset($rs['_curl_info']);

        return $rs;
    }

    /**
     * Replaces a collection.
     *
     * @param array  $data Collection data
     * @param string $id
     *
     * @return array
     */
    public function replaceCollection(array $data, $id)
    {
        $coll = new Collection($this->client, $this->database);
        $rs = $coll->replace($data, $id);
        unset($rs['_curl_info']);

        return $rs;
    }

    /**
     * Deletes a collection.
     *
     * @param string $id Collection ID
     *
     * @return array
     */
    public function deleteCollection($id)
    {
        $coll = new Collection($this->client, $this->database);
        $rs = $coll->delete($id);
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
     *
     * @return array
     */
    public function listDocuments($collection)
    {
        $doc = new Document($this->client, $this->database, $collection);
        $rs = $doc->list();
        unset($rs['_curl_info']);

        return $rs;
    }

    /**
     * Retrieves a document.
     *
     * @param string $collection Collection to list document from
     * @param string $id         ID of the document to retrieve
     *
     * @return array
     */
    public function getDocument($collection, $id)
    {
        $doc = new Document($this->client, $this->database, $collection);
        $rs = $doc->get($id);
        unset($rs['_curl_info']);

        return $rs;
    }

    /**
     * Creates a document.
     *
     * @param string $collection Collection to create a document in
     * @param array  $data       Document data
     *
     * @return array
     */
    public function createDocument($collection, array $data)
    {
        $doc = new Document($this->client, $this->database, $collection);
        $rs = $doc->create($data);
        unset($rs['_curl_info']);

        return $rs;
    }

    /**
     * Replaces a document.
     *
     * @param string $collection Collection to replace a document in
     * @param array  $data       Document data
     * @param string $id         ID of the document being replaced
     *
     * @return array
     */
    public function replaceDocument($collection, array $data, $id)
    {
        $doc = new Document($this->client, $this->database, $collection);
        $rs = $doc->replace($data, $id);
        unset($rs['_curl_info']);

        return $rs;
    }

    /**
     * Deletes a document.
     *
     * @param $collection Collection to delete a document from
     * @param $id         ID of the document to delete
     *
     * @return array
     */
    public function deleteDocument($collection, $id)
    {
        $doc = new Document($this->client, $this->database, $collection);
        $rs = $doc->delete($id);
        unset($rs['_curl_info']);

        return $rs;
    }

    /**
     * Queries a collection for documents.
     *
     * @param string $collection Collection to perform the query on
     * @param string $sql        SQL query string
     * @param array  $params     Query parameters
     *
     * @return array
     */
    public function queryDocument($collection, $sql, array $params = [])
    {
        $doc = new Document($this->client, $this->database, $collection);
        $rs = $doc->query($sql, $params);
        unset($rs['_curl_info']);

        return $rs;
    }
}