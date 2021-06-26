<?php

declare(strict_types=1);

namespace Symfony\Component\Messenger\Bridge\Bunny\Tests\Transport;

use PHPUnit\Framework\TestCase;
use Symfony\Component\Messenger\Bridge\Bunny\Transport\BunnyTransport;
use Symfony\Component\Messenger\Bridge\Bunny\Transport\BunnyTransportFactory;
use Symfony\Component\Messenger\Bridge\Bunny\Transport\Connection;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;

class BunnyTransportFactoryTest extends TestCase
{
    public function testItSupportsOnlyBunnyTransports()
    {
        $factory = new BunnyTransportFactory();

        $this->assertTrue($factory->supports('amqp+bunny://localhost', []));
        $this->assertFalse($factory->supports('sqs://localhost', []));
        $this->assertFalse($factory->supports('invalid-dsn', []));
    }

    public function testItCreatesTheTransport()
    {
        $factory = new BunnyTransportFactory();
        $serializer = $this->createMock(SerializerInterface::class);

        $expectedTransport = new BunnyTransport(Connection::fromDsn('amqp+bunny://localhost', ['host' => 'localhost']), $serializer);

        $this->assertEquals($expectedTransport, $factory->createTransport('amqp://localhost', ['host' => 'localhost'], $serializer));
    }
}
