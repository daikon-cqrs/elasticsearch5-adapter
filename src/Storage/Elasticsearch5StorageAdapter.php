<?php

namespace Daikon\Elasticsearch5\Storage;

use Daikon\Dbal\Exception\DbalException;
use Daikon\Dbal\Storage\StorageAdapterInterface;
use Daikon\Elasticsearch5\Connector\Elasticsearch5Connector;
use Daikon\ReadModel\Projection\ProjectionMap;
use Elasticsearch\Common\Exceptions\Missing404Exception;

final class Elasticsearch5StorageAdapter implements StorageAdapterInterface
{
    private $connector;

    private $settings;

    public function __construct(Elasticsearch5Connector $connector, array $settings = [])
    {
        $this->connector = $connector;
        $this->settings = $settings;
    }

    public function read(string $identifier)
    {
        try {
            $document = $this->connector->getConnection()->get([
                'index' => $this->getIndex(),
                'type' => $this->settings['type'],
                'id' => $identifier
            ]);
        } catch (Missing404Exception $error) {
            return null;
        }

        $projectionClass = $document['_source']['@type'];
        return $projectionClass::fromArray($document['_source']);
    }

    public function write(string $identifier, array $data)
    {
        $document = [
            'index' => $this->getIndex(),
            'type' => $this->settings['type'],
            'id' => $identifier,
            'body' => $data
        ];

        $this->connector->getConnection()->index($document);

        return true;
    }

    public function search(array $query, $from, $size)
    {
        $query = array_merge(
            $query,
            [
                'index' => $this->getIndex(),
                'type' => $this->settings['type'],
                'from' => $from,
                'size' => $size
            ]
        );

        $results = $this->connector->getConnection()->search($query);

        $projections = [];
        foreach ($results['hits']['hits'] as $document) {
            $projectionClass = $document['_source']['@type'];
            $projections[$document['_id']] = $projectionClass::fromArray($document['_source']);
        }

        return new ProjectionMap($projections);
    }

    public function delete(string $identifier)
    {
        throw new DbalException('Not yet implemented');
    }

    private function getIndex(): string
    {
        return $this->settings['index'] ?? $this->connector->getSettings()['index'];
    }
}
