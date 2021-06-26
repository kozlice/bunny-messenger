<?php

declare(strict_types=1);

namespace Symfony\Component\Messenger\Bridge\Bunny\Transport;

use Bunny\Message as BunnyMessage;
use Symfony\Component\Messenger\Stamp\NonSendableStampInterface;

final class BunnyReceivedStamp implements NonSendableStampInterface
{
    private $bunnyMessage;
    private $queueName;

    public function __construct(BunnyMessage $message, string $queueName)
    {
        $this->bunnyMessage = $message;
        $this->queueName = $queueName;
    }

    public function getBunnyMessage(): BunnyMessage
    {
        return $this->bunnyMessage;
    }

    public function getQueueName(): string
    {
        return $this->queueName;
    }
}
