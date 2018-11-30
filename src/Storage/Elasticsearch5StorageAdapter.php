<?php
/**
 * This file is part of the daikon-cqrs/elasticsearch5-adapter project.
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace Daikon\Elasticsearch5\Storage;

use Daikon\Dbal\Exception\DbalException;
use Daikon\Elasticsearch5\Connector\Elasticsearch5Connector;
use Daikon\ReadModel\Projection\ProjectionInterface;
use Daikon\ReadModel\Projection\ProjectionMap;
use Daikon\ReadModel\Query\QueryInterface;
use Daikon\ReadModel\Storage\SearchAdapterInterface;
use Daikon\ReadModel\Storage\StorageAdapterInterface;
use Elasticsearch\Common\Exceptions\Missing404Exception;

final class Elasticsearch5StorageAdapter implements StorageAdapterInterface, SearchAdapterInterface
{
    /** @var Elasticsearch5Connector */
    private $connector;

    /** @var array */
    private $settings;

    public function __construct(Elasticsearch5Connector $connector, array $settings = [])
    {
        $this->connector = $connector;
        $this->settings = $settings;
    }

    public function read(string $identifier): ?ProjectionInterface
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
        return $projectionClass::fromNative($document['_source']);
    }

    public function write(string $identifier, array $data): bool
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

    public function delete(string $identifier): bool
    {
        throw new DbalException('Not yet implemented');
    }

    public function search(QueryInterface $query, int $from = null, int $size = null): ProjectionMap
    {
        $query = [
            'index' => $this->getIndex(),
            'type' => $this->settings['type'],
            'from' => $from,
            'size' => $size,
            'body' => $query->toNative()
        ];

        $results = $this->connector->getConnection()->search($query);

        $projections = [];
        foreach ($results['hits']['hits'] as $document) {
            $projectionClass = $document['_source']['@type'];
            $projections[$document['_id']] = $projectionClass::fromNative($document['_source']);
        }

        return new ProjectionMap($projections);
    }

    private function getIndex(): string
    {
        return $this->settings['index'] ?? $this->connector->getSettings()['index'];
    }
}
