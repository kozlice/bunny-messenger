<?php

declare(strict_types=1);

namespace Symfony\Component\Messenger\Bridge\Bunny\Tests\Transport;

use Bunny\Exception\ClientException;
use Bunny\Message as BunnyMessage;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Messenger\Bridge\Bunny\Tests\Fixtures\DummyMessage;
use Symfony\Component\Messenger\Bridge\Bunny\Transport\BunnyReceivedStamp;
use Symfony\Component\Messenger\Bridge\Bunny\Transport\BunnyReceiver;
use Symfony\Component\Messenger\Bridge\Bunny\Transport\Connection;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Exception\LogicException;
use Symfony\Component\Messenger\Exception\MessageDecodingFailedException;
use Symfony\Component\Messenger\Exception\TransportException;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;

class BunnyReceiverTest extends TestCase
{
    private function createBunnyMessage(): BunnyMessage
    {
        return new BunnyMessage('', 1, false, 'test', '', [], '{}');
    }

    private function getEnvelopeWithBunnyReceivedStamp(BunnyMessage $bunnyMessage): Envelope
    {
        $message = new DummyMessage('hi');
        $stamp = new BunnyReceivedStamp($bunnyMessage, 'test');

        return Envelope::wrap($message)->with($stamp);
    }

    public function testItAcknowledgesTheMessage()
    {
        $bunnyMessage = $this->createBunnyMessage();
        $connection = $this->createMock(Connection::class);
        $connection
            ->expects($this->once())
            ->method('ack')
            ->with($bunnyMessage)
        ;

        $receiver = new BunnyReceiver($connection);
        $receiver->ack($this->getEnvelopeWithBunnyReceivedStamp($bunnyMessage));
    }

    public function testItThrowsIfAcknowledgeFails()
    {
        $bunnyMessage = $this->createBunnyMessage();
        $connection = $this->createMock(Connection::class);
        $connection
            ->expects($this->once())
            ->method('ack')
            ->willThrowException(new ClientException())
        ;

        $this->expectException(TransportException::class);

        $receiver = new BunnyReceiver($connection);
        $receiver->ack($this->getEnvelopeWithBunnyReceivedStamp($bunnyMessage));
    }

    public function testItRejectsTheMessage()
    {
        $bunnyMessage = $this->createBunnyMessage();
        $connection = $this->createMock(Connection::class);
        $connection
            ->expects($this->once())
            ->method('nack')
            ->with($bunnyMessage)
        ;

        $receiver = new BunnyReceiver($connection);
        $receiver->reject($this->getEnvelopeWithBunnyReceivedStamp($bunnyMessage));
    }

    public function testItThrowsIfRejectionFails()
    {
        $bunnyMessage = $this->createBunnyMessage();
        $connection = $this->createMock(Connection::class);
        $connection
            ->expects($this->once())
            ->method('nack')
            ->willThrowException(new ClientException())
        ;

        $this->expectException(TransportException::class);

        $receiver = new BunnyReceiver($connection);
        $receiver->reject($this->getEnvelopeWithBunnyReceivedStamp($bunnyMessage));
    }

    public function testItThrowsIfBunnyReceivedStampNotFound()
    {
        $connection = $this->createMock(Connection::class);

        $this->expectException(LogicException::class);
        $this->expectExceptionMessage('No "BunnyReceivedStamp" stamp found on the Envelope.');

        $receiver = new BunnyReceiver($connection);
        $receiver->ack(Envelope::wrap(new DummyMessage('hi')));
    }

    public function testItGetsMessageCount()
    {
        $connection = $this->createMock(Connection::class);
        $connection
            ->expects($this->once())
            ->method('countMessagesInQueues')
            ->willReturn(35)
        ;

        $receiver = new BunnyReceiver($connection);
        $this->assertEquals(35, $receiver->getMessageCount());
    }

    public function testItThrowsIfMessageCountFails()
    {
        $connection = $this->createMock(Connection::class);
        $connection
            ->expects($this->once())
            ->method('countMessagesInQueues')
            ->willThrowException(new ClientException())
        ;

        $this->expectException(TransportException::class);

        $receiver = new BunnyReceiver($connection);
        $receiver->getMessageCount();
    }

    private function getApplicationHeaders(): array
    {
        return [
            'type' => 'DummyMessage',
            'X-Message-Stamp-Symfony\Component\Messenger\Stamp\BusNameStamp' => '[{"busName":"event.bus"}]',
        ];
    }

    private function generateBunnyMessages(): iterable
    {
        $headers = $this->getApplicationHeaders() + [
            'delivery-mode' => 2,
            'priority' => 10,
            'app-id' => 'my-app',
            '__source_queue_name__' => 'queue_0',
        ];

        for ($i = 0; $i < 3; $i++) {
            $bunnyMessage = $this->createBunnyMessage();
            $bunnyMessage->headers = $headers;
            yield $bunnyMessage;
        }
    }

    public function testItGetsMessagesFromAllQueues()
    {
        $connection = $this->createMock(Connection::class);
        $connection
            ->expects($this->once())
            ->method('getQueueNames')
            ->willReturn(['queue_0', 'queue_1'])
        ;
        $connection
            ->expects($this->once())
            ->method('get')
            ->with(['queue_0', 'queue_1'])
            ->willReturn($this->generateBunnyMessages())
        ;
        $serializer = $this->createMock(SerializerInterface::class);
        $serializer
            ->expects($this->exactly(3))
            ->method('decode')
            ->withConsecutive(
                [['body' => '{}', 'headers' => $this->getApplicationHeaders()]],
                [['body' => '{}', 'headers' => $this->getApplicationHeaders()]],
                [['body' => '{}', 'headers' => $this->getApplicationHeaders()]]
            )
            ->willReturnOnConsecutiveCalls(
                Envelope::wrap(new DummyMessage('hi')),
                Envelope::wrap(new DummyMessage('hi')),
                Envelope::wrap(new DummyMessage('hi'))
            )
        ;

        $receiver = new BunnyReceiver($connection, $serializer);
        $envelopes = iterator_to_array($receiver->get());
        $this->assertContainsOnlyInstancesOf(Envelope::class, $envelopes);
        foreach ($envelopes as $envelope) {
            $stamp = $envelope->last(BunnyReceivedStamp::class);
            $this->assertInstanceOf(BunnyReceivedStamp::class, $stamp);
            $this->assertInstanceOf(BunnyMessage::class, $stamp->getBunnyMessage());
            $this->assertEquals('queue_0', $stamp->getQueueName());
        }
    }

    public function testItRejectsMessageIfDecodingFailsAndThrows()
    {
        $connection = $this->createMock(Connection::class);
        $connection
            ->expects($this->once())
            ->method('get')
            ->with(['queue_0', 'queue_1'])
            ->willReturn($this->generateBunnyMessages())
        ;
        $connection
            ->expects($this->once())
            ->method('nack')
        ;
        $serializer = $this->createMock(SerializerInterface::class);
        $serializer
            ->expects($this->once())
            ->method('decode')
            ->willThrowException(new MessageDecodingFailedException('Decoding failed'))
        ;

        $this->expectException(MessageDecodingFailedException::class);

        $receiver = new BunnyReceiver($connection, $serializer);
        $receiver->getFromQueues(['queue_0', 'queue_1'])->current();
    }
}
