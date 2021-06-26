<?php

declare(strict_types=1);

namespace Symfony\Component\Messenger\Bridge\Bunny\Transport;

use Bunny\Exception\ClientException;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Exception\TransportException;
use Symfony\Component\Messenger\Stamp\DelayStamp;
use Symfony\Component\Messenger\Stamp\RedeliveryStamp;
use Symfony\Component\Messenger\Transport\Sender\SenderInterface;
use Symfony\Component\Messenger\Transport\Serialization\PhpSerializer;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;

class BunnySender implements SenderInterface
{
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
    public function send(Envelope $envelope): Envelope
    {
        $encodedMessage = $this->serializer->encode($envelope);

        /** @var DelayStamp|null $delayStamp */
        $delayStamp = $envelope->last(DelayStamp::class);
        $delay = $delayStamp ? $delayStamp->getDelay() : 0;

        /** @var BunnyStamp|null $bunnyStamp */
        $bunnyStamp = $envelope->last(BunnyStamp::class);
        if (isset($encodedMessage['headers']['Content-Type'])) {
            $contentType = $encodedMessage['headers']['Content-Type'];
            unset($encodedMessage['headers']['Content-Type']);

            if (!$bunnyStamp || !isset($bunnyStamp->getAttributes()['content-type'])) {
                $bunnyStamp = BunnyStamp::createWithAttributes(['content-type' => $contentType], $bunnyStamp);
            }
        }

        // TODO: PriorityStamp when it's merged into Symfony.

        $bunnyReceivedStamp = $envelope->last(BunnyReceivedStamp::class);
        if ($bunnyReceivedStamp instanceof BunnyReceivedStamp) {
            $bunnyStamp = BunnyStamp::createFromBunnyMessage(
                $bunnyReceivedStamp->getBunnyMessage(),
                $bunnyStamp,
                $envelope->last(RedeliveryStamp::class) ? $bunnyReceivedStamp->getQueueName() : null
            );
        }

        try {
            $this->connection->publish(
                $encodedMessage['body'],
                $encodedMessage['headers'] ?? [],
                $delay,
                $bunnyStamp
            );
        } catch (ClientException $e) {
            throw new TransportException($e->getMessage(), 0, $e);
        }

        return $envelope;
    }
}
