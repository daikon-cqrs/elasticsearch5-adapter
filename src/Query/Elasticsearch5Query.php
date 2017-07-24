<?php

namespace Daikon\Elasticsearch5\Query;

use Daikon\ReadModel\Query\QueryInterface;

final class Elasticsearch5Query implements QueryInterface
{
    private $query;

    public function __construct(array $query = [])
    {
        $this->query = $query;
    }

    public function toNative()
    {
        return $this->query;
    }
}
