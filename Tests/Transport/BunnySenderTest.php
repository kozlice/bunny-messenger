<?php

declare(strict_types=1);

namespace Symfony\Component\Messenger\Bridge\Bunny\Tests\Transport;

use Bunny\Exception\ClientException;
use Bunny\Message as BunnyMessage;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Messenger\Bridge\Bunny\Tests\Fixtures\DummyMessage;
use Symfony\Component\Messenger\Bridge\Bunny\Transport\BunnyReceivedStamp;
use Symfony\Component\Messenger\Bridge\Bunny\Transport\BunnySender;
use Symfony\Component\Messenger\Bridge\Bunny\Transport\BunnyStamp;
use Symfony\Component\Messenger\Bridge\Bunny\Transport\Connection;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Exception\TransportException;
use Symfony\Component\Messenger\Stamp\DelayStamp;
use Symfony\Component\Messenger\Stamp\RedeliveryStamp;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;

class BunnySenderTest extends TestCase
{
    public function testItSendsTheEncodedMessage()
    {
        $envelope = Envelope::wrap(new DummyMessage('Hi'));
        $encoded = ['body' => '...', 'headers' => ['type' => DummyMessage::class]];

        $serializer = $this->createMock(SerializerInterface::class);
        $serializer->method('encode')->with($envelope)->willReturnOnConsecutiveCalls($encoded);

        $connection = $this->createMock(Connection::class);
        $connection->expects($this->once())->method('publish')->with($encoded['body'], $encoded['headers']);

        $sender = new BunnySender($connection, $serializer);
        $sender->send($envelope);
    }

    public function testItSendsTheEncodedMessageWithARoutingKey()
    {
        $envelope = Envelope::wrap(new DummyMessage('Hi'))
            ->with($stamp = new BunnyStamp('my-key'))
        ;
        $encoded = ['body' => '...', 'headers' => ['type' => DummyMessage::class]];

        $serializer = $this->createMock(SerializerInterface::class);
        $serializer->method('encode')->with($envelope)->willReturnOnConsecutiveCalls($encoded);

        $connection = $this->createMock(Connection::class);
        $connection->expects($this->once())->method('publish')->with($encoded['body'], $encoded['headers'], 0, $stamp);

        $sender = new BunnySender($connection, $serializer);
        $sender->send($envelope);
    }

    public function testItSendsTheEncodedMessageWithoutHeaders()
    {
        $envelope = Envelope::wrap(new DummyMessage('Hi'));
        $encoded = ['body' => '...'];

        $serializer = $this->createMock(SerializerInterface::class);
        $serializer->method('encode')->with($envelope)->willReturnOnConsecutiveCalls($encoded);

        $connection = $this->createMock(Connection::class);
        $connection->expects($this->once())->method('publish')->with($encoded['body'], []);

        $sender = new BunnySender($connection, $serializer);
        $sender->send($envelope);
    }

    public function testContentTypeHeaderIsMovedToAttribute()
    {
        $envelope = Envelope::wrap(new DummyMessage('Hi'));
        $encoded = ['body' => '...', 'headers' => ['type' => DummyMessage::class, 'Content-Type' => 'application/json']];

        $serializer = $this->createMock(SerializerInterface::class);
        $serializer->method('encode')->with($envelope)->willReturnOnConsecutiveCalls($encoded);

        $connection = $this->createMock(Connection::class);
        unset($encoded['headers']['Content-Type']);
        $stamp = new BunnyStamp('', ['content-type' => 'application/json']);
        $connection->expects($this->once())->method('publish')->with($encoded['body'], $encoded['headers'], 0, $stamp);

        $sender = new BunnySender($connection, $serializer);
        $sender->send($envelope);
    }

    public function testContentTypeHeaderDoesNotOverwriteAttribute()
    {
        $envelope = Envelope::wrap(new DummyMessage('Hi'))
            ->with($stamp = new BunnyStamp('my-key', ['content-type' => 'custom']))
        ;
        $encoded = ['body' => '...', 'headers' => ['type' => DummyMessage::class, 'Content-Type' => 'application/json']];

        $serializer = $this->createMock(SerializerInterface::class);
        $serializer->method('encode')->with($envelope)->willReturnOnConsecutiveCalls($encoded);

        $connection = $this->createMock(Connection::class);
        unset($encoded['headers']['Content-Type']);
        $connection->expects($this->once())->method('publish')->with($encoded['body'], $encoded['headers'], 0, $stamp);

        $sender = new BunnySender($connection, $serializer);
        $sender->send($envelope);
    }

    public function testItThrowsATransportExceptionIfItCannotSendTheMessage()
    {
        $this->expectException(TransportException::class);
        $envelope = new Envelope(new DummyMessage('Hi'));
        $encoded = ['body' => '...', 'headers' => ['type' => DummyMessage::class]];

        $serializer = $this->createMock(SerializerInterface::class);
        $serializer->method('encode')->with($envelope)->willReturnOnConsecutiveCalls($encoded);

        $connection = $this->createMock(Connection::class);
        $connection->method('publish')->with($encoded['body'], $encoded['headers'])->willThrowException(new ClientException());

        $sender = new BunnySender($connection, $serializer);
        $sender->send($envelope);
    }

    public function testItSendsWithRetryRoutingKey()
    {
        $bunnyMessage = new BunnyMessage('', 1, false, '', '', [], '{}');
        $envelope = Envelope::wrap(new DummyMessage('Hi'))
            ->with(new BunnyReceivedStamp($bunnyMessage, 'queue_0'))
            ->with(new RedeliveryStamp(1))
        ;
        $encoded = ['body' => '...', 'headers' => ['type' => DummyMessage::class]];

        $serializer = $this->createMock(SerializerInterface::class);
        $serializer->method('encode')->with($envelope)->willReturnOnConsecutiveCalls($encoded);

        $connection = $this->createMock(Connection::class);
        $connection
            ->expects($this->once())
            ->method('publish')
            ->with(
                $encoded['body'],
                $encoded['headers'],
                0,
                $this->callback(function (BunnyStamp $stamp) {
                    $this->assertEquals('queue_0', $stamp->getRoutingKey());
                    $this->assertTrue($stamp->isRetryAttempt());

                    return true;
                })
            )
        ;

        $sender = new BunnySender($connection, $serializer);
        $sender->send($envelope);
    }

    public function testItSendsWithDelay()
    {
        $envelope = Envelope::wrap(new DummyMessage('Hi'))
            ->with(new DelayStamp(1000))
        ;
        $encoded = ['body' => '...', 'headers' => ['type' => DummyMessage::class]];

        $serializer = $this->createMock(SerializerInterface::class);
        $serializer->method('encode')->with($envelope)->willReturnOnConsecutiveCalls($encoded);

        $connection = $this->createMock(Connection::class);
        $connection->expects($this->once())->method('publish')->with($encoded['body'], $encoded['headers'], 1000);

        $sender = new BunnySender($connection, $serializer);
        $sender->send($envelope);
    }

    // TODO: testSendWithPriority when it's merged.
}
