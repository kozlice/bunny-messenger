<?php

declare(strict_types=1);

namespace Symfony\Component\Messenger\Bridge\Bunny\Transport;

use Bunny\Exception\ClientException;
use Bunny\Message as BunnyMessage;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Exception\LogicException;
use Symfony\Component\Messenger\Exception\MessageDecodingFailedException;
use Symfony\Component\Messenger\Exception\TransportException;
use Symfony\Component\Messenger\Transport\Receiver\MessageCountAwareInterface;
use Symfony\Component\Messenger\Transport\Receiver\QueueReceiverInterface;
use Symfony\Component\Messenger\Transport\Serialization\PhpSerializer;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;

class BunnyReceiver implements QueueReceiverInterface, MessageCountAwareInterface
{
    // Bunny does not segregate application headers from built-in properties.
    // Everything that is not a built-in property is considered a header.
    // When we receive message, we need to filter them out.
    // However, `type` property must be kept, because it's used by messenger
    // to store message class name.
    private const NON_APPLICATION_HEADERS = [
        'delivery-mode',
        'content-type',
        'content-encoding',
        'priority',
        'correlation-id',
        'reply-to',
        'expiration',
        'message-id',
        'timestamp',
        'user-id',
        'app-id',
        'cluster-id',
    ];

    private $serializer;
    private $connection;

    public function __construct(Connection $connection, SerializerInterface $serializer = null)
    {
        $this->connection = $connection;
        $this->serializer = $serializer ?? new PhpSerializer();
    }

    /**
     * {@inheritdoc}
     */
    public function get(): iterable
    {
        yield from $this->getFromQueues($this->connection->getQueueNames());
    }

    /**
     * {@inheritdoc}
     */
    public function getFromQueues(array $queueNames): iterable
    {
        $generator = $this->connection->get($queueNames);

        // TODO: Where to handle ClientException? Outside foreach?
        /** @var BunnyMessage $bunnyMessage */
        foreach ($generator as $bunnyMessage) {
            try {
                $queueName = $bunnyMessage->getHeader('__source_queue_name__');
                unset($bunnyMessage->headers['__source_queue_name__']);
                $envelope = $this->serializer->decode([
                    'body' => $bunnyMessage->content,
                    'headers' => $this->getApplicationHeaders($bunnyMessage),
                ]);
            } catch (MessageDecodingFailedException $exception) {
                $this->connection->nack($bunnyMessage);

                throw $exception;
            }

            yield $envelope->with(new BunnyReceivedStamp($bunnyMessage, $queueName));
        }
    }

    /**
     * {@inheritdoc}
     */
    public function ack(Envelope $envelope): void
    {
        try {
            $stamp = $this->findBunnyStamp($envelope);
            $this->connection->ack($stamp->getBunnyMessage());
        } catch (ClientException $exception) {
            throw new TransportException($exception->getMessage(), 0, $exception);
        }
    }

    /**
     * {@inheritdoc}
     */
    public function reject(Envelope $envelope): void
    {
        try {
            $stamp = $this->findBunnyStamp($envelope);
            $this->connection->nack($stamp->getBunnyMessage());
        } catch (ClientException $exception) {
            throw new TransportException($exception->getMessage(), 0, $exception);
        }
    }

    private function findBunnyStamp(Envelope $envelope): BunnyReceivedStamp
    {
        $stamp = $envelope->last(BunnyReceivedStamp::class);
        if (null === $stamp) {
            throw new LogicException('No "BunnyReceivedStamp" stamp found on the Envelope.');
        }

        return $stamp;
    }

    /**
     * {@inheritdoc}
     */
    public function getMessageCount(): int
    {
        try {
            return $this->connection->countMessagesInQueues();
        } catch (ClientException $exception) {
            throw new TransportException($exception->getMessage(), 0, $exception);
        }
    }

    private function getApplicationHeaders(BunnyMessage $message): array
    {
        return array_diff_key($message->headers, array_flip(self::NON_APPLICATION_HEADERS));
    }
}
