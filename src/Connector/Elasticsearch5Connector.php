<?php

namespace Daikon\Elasticsearch5\Connector;

use Daikon\Dbal\Connector\ConnectorInterface;
use Daikon\Dbal\Connector\ConnectorTrait;
use Elasticsearch\Client;
use Elasticsearch\ClientBuilder;

final class Elasticsearch5Connector implements ConnectorInterface
{
    use ConnectorTrait;

    private function connect(): Client
    {
        $connectionDsn = [
            'scheme' => $this->settings['scheme'],
            'host' => $this->settings['host'],
            'port' => $this->settings['port'],
            'user' => $this->settings['user'],
            'pass' => $this->settings['password']
        ];

        return ClientBuilder::create()
            ->setHosts([$connectionDsn])
            ->build();
    }
}
