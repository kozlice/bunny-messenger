<?php

declare(strict_types=1);

namespace Symfony\Component\Messenger\Bridge\Bunny\Transport;

use Bunny\Client;

class BunnyFactory
{
    public function createConnection(array $config): Client
    {
        return new Client($config);
    }

    public function createBuffer(): \SplQueue
    {
        return new \SplQueue();
    }
}
