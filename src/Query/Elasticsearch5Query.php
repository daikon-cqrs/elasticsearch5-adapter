<?php

namespace Daikon\Elasticsearch5\Query;

use Daikon\ReadModel\Query\QueryInterface;

final class Elasticsearch5Query implements QueryInterface
{
    /** @var array */
    private $query;

    /** @param array $query */
    public function fromNative($query): QueryInterface
    {
        return new self($query);
    }

    public function toNative(): array
    {
        return $this->query;
    }

    private function __construct(array $query = [])
    {
        $this->query = $query;
    }
}
