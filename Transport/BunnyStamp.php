<?php

declare(strict_types=1);

namespace Symfony\Component\Messenger\Bridge\Bunny\Transport;

use Bunny\Message as BunnyMessage;
use Symfony\Component\Messenger\Stamp\NonSendableStampInterface;

class BunnyStamp implements NonSendableStampInterface
{
    private $routingKey;
    private $attributes;
    private $isRetryAttempt = false;

    public function __construct(string $routingKey = '', array $attributes = [])
    {
        $this->routingKey = $routingKey;
        $this->attributes = $attributes;
    }

    public function getRoutingKey(): string
    {
        return $this->routingKey;
    }

    public function getAttributes(): array
    {
        return $this->attributes;
    }

    public function isRetryAttempt(): bool
    {
        return $this->isRetryAttempt;
    }

    public static function createFromBunnyMessage(BunnyMessage $bunnyMessage, self $previousStamp = null, string $retryRoutingKey = null): self
    {
        $attributes = array_merge($bunnyMessage->headers, $previousStamp->attributes ?? []);
        $routingKey = $retryRoutingKey ?? $previousStamp->routingKey ?? $bunnyMessage->routingKey ?? '';

        $stamp = new self($routingKey, $attributes);
        $stamp->isRetryAttempt = null !== $retryRoutingKey;

        return $stamp;
    }

    public static function createWithAttributes(array $attributes, self $previousStamp = null): self
    {
        return new self(
            $previousStamp->routingKey ?? '',
            array_merge($previousStamp->attributes ?? [], $attributes)
        );
    }
}
