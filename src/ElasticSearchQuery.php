<?php

/**
 * Created by PhpStorm.
 * User: BanBan
 * Date: 2020/11/23
 * Time: 13:20
 */


namespace Banbans\LaravelScoutElastic;

use Elasticsearch\ClientBuilder;
use Illuminate\Container\Container;
use Illuminate\Pagination\LengthAwarePaginator;
use Illuminate\Pagination\Paginator;

class ElasticSearchQuery{


    private $hosts;

    private $client;

    private $index;

    private $model;

    private $from;

    private $size;

    private $body;

    private $query = [];

    private $aggs = [];

    private $source = [];

    static $modelClass;


    public function __construct()
    {

        $this->hosts = config('scout.elasticsearch.hosts');

        $this->client = ClientBuilder::create()->setHosts($this->hosts)->build();

        $this->from = 0;

        $this->size = 10;


    }

    /**
     * 指定索引
     * @param $class
     * @return $this
     */
    public function index($class)
    {

        self::$modelClass = $class;

        $this->model = app($class);

        $this->index = $this->model->getTable();

        return $this;
    }


    /**
     * 分页获取数据库数据
     * @param null $perPage
     * @param string $page_name
     * @param null $page
     * @return mixed
     */
    public function paginate($perPage = null ,$page_name = 'page',$page=null)
    {

        $page = $page ?: Paginator::resolveCurrentPage('page');

        $perPage = $perPage ?: $this->model->getPerPage();

        $this->from = $page*($page-1);

        $this->size = $perPage;


        $results = $this->search();

        $list =  $this->map($results);

        $paginator = Container::getInstance()->makeWith(LengthAwarePaginator::class, [
            'items' => $list,
            'total' =>  $this->getTotalCount($results),
            'perPage' => $perPage,
            'currentPage' => $page,
            'options' => [
                'path' => Paginator::resolveCurrentPath(),
                'pageName' => 'page',
            ],
        ]);
        return $paginator;
    }


    /**
     * 分页获取es数据
     * @param null $perPage
     * @param string $page_name
     * @param null $page
     * @return mixed
     */
    public function paginateRaw($perPage = null ,$page_name = 'page',$page=null)
    {

        $page = $page ?: Paginator::resolveCurrentPage('page');

        $perPage = $perPage ?: $this->model->getPerPage();

        $this->from = $page*($page-1);

        $this->size = $perPage;

        $results = $this->search();

        $list =  $results['hits']['hits'];

        $paginator = Container::getInstance()->makeWith(LengthAwarePaginator::class, [
            'items' => $list,
            'total' =>  $this->getTotalCount($results),
            'perPage' => $perPage,
            'currentPage' => $page,
            'options' => [
                'path' => Paginator::resolveCurrentPath(),
                'pageName' => 'page',
            ],
        ]);
        return $paginator;
    }


    /**
     * 数据总条数
     * @param $results
     * @return int
     */
    public function getTotalCount($results)
    {
        return intval($results['hits']['total']['value']);
    }

    /**
     * 根据es查询结果获取数据库数据
     * @param $results
     * @return mixed
     */
    public function map($results)
    {
        if ($this->getTotalCount($results) === 0) {

            return $this->model->newCollection();
        }

        $keys = collect($results['hits']['hits'])->pluck('_id')->values()->all();


        $model=new  $this->model();

        $list = self::$modelClass ::where(function($query) use($keys,$model){

            return $query->whereIn(
                $model->getScoutKeyName(), $keys
            );

        })->select('org_id')->get();

        return $list;


    }


    /**
     * 指定查询字段
     * @param array $filed
     * @return $this
     */
    public function field(array $filed=[])
    {
        $this->source=$filed;

        return $this;
    }


    /**
     * 查询请求
     * @return array
     */
    public function search()
    {

        $this->body=[
            'from'=>$this->from,
            'size'=>$this->size,
        ];

        if(!empty($this->query)){
            $this->body["query"]=$this->query;
        }

        if(!empty($this->source)){
            $this->body["_source"]=$this->source;
        }

        if(!empty($this->aggs)){
            $this->body["aggs"]=$this->aggs;
        }

        $params = [
            'index' => $this->index,
            'type' => '_doc',
            'body' => $this->body
        ];


        $results = $this->client->search($params);

        return $results;
    }


    /**
     * 自定义查询语句
     * @param array $params
     * @return array
     */
    public function seclectQuery(array $params)
    {

        $this->body=$params;

        $map = [
            'index' => $this->index,
            'type' => '_doc',
            'body' => $this->body
        ];

        $results = $this->client->search($map);

        return $results;

    }


    /**
     * 查询条件
     * @param array $query
     * @return $this
     */
    public function query(array $query = [])
    {

        $this->query = $query;

        return $this;

    }


    /**
     * 聚合查询
     * @param array $aggs
     * @return $this
     */
    public function aggs(array $aggs = [])
    {

        $this->aggs=$aggs;

        return $this;

    }

}