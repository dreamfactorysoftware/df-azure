<?php

namespace DreamFactory\Core\Azure\Components;

use DreamFactory\Core\Exceptions\InternalServerErrorException;
use DreamFactory\DocumentDb\Client;
use DreamFactory\DocumentDb\Resources\Collection;

class DocumentDBConnection
{
    /** @var Client  */
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

    public function listCollections()
    {
        $coll = new Collection($this->client, $this->database);
        $rs = $coll->list();
        $colls = array_get($rs, 'DocumentCollections');
        $list = [];
        if(!empty($colls)){
            foreach ($colls as $coll){
                $list[] = array_get($coll, 'id');
            }
        }

        return $list;
    }

    public function getCollection($id)
    {
        $coll = new Collection($this->client, $this->database);
        $rs = $coll->get($id);
        unset($rs['_curl_info']);
        return $rs;
    }

    public function createCollection(array $data)
    {
        $coll = new Collection($this->client, $this->database);
        $rs = $coll->create($data);
        unset($rs['_curl_info']);
        return $rs;
    }

    public function replaceCollection(array $data, $id)
    {
        $coll = new Collection($this->client, $this->database);
        $rs = $coll->replace($data, $id);
        unset($rs['_curl_info']);
        return $rs;
    }

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

    public function listDocuments()
    {

    }

    public function getDocument()
    {

    }

    public function createDocument()
    {

    }

    public function replaceDocument()
    {

    }

    public function deleteDocument()
    {

    }

    public function queryDocument()
    {

    }
}