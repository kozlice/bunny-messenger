<?php

declare(strict_types=1);

namespace Symfony\Component\Messenger\Bridge\Bunny\Transport;

use Bunny\Channel;
use Bunny\Client;
use Bunny\Message as BunnyMessage;
use Symfony\Component\Messenger\Exception\InvalidArgumentException;

class Connection
{
    private const QUEUE_INTEGER_ARGUMENTS = [
        'x-delay',
        'x-expires',
        'x-max-length',
        'x-max-length-bytes',
        'x-max-priority',
        'x-message-ttl',
    ];

    private $configuration;
    private $factory;
    private $client = null;
    private $channel = null;
    private $buffer = null;
    private $consumerTags = [];
    private $autoSetup;
    private $waitTime;

    public function __construct(array $options, ?BunnyFactory $factory = null)
    {
        $configuration = [];

        // Build client configuration
        $configuration['client'] = [
            'host' => $options['host'] ?? 'localhost',
            'port' => $options['port'] ?? (isset($options['ssl']) ? 5671 : 5672),
            'user' => $options['user'] ?? 'guest',
            'password' => $options['password'] ?? 'guest',
            'vhost' => $options['vhost'] ?? '/',
        ];

        if (array_key_exists('heartbeat', $options)) {
            $configuration['client']['heartbeat'] = filter_var($options['heartbeat'], FILTER_VALIDATE_FLOAT);
        }

        if (array_key_exists('connection_timeout', $options)) {
            $configuration['client']['timeout'] = filter_var($options['connection_timeout'], FILTER_VALIDATE_FLOAT);
        }

        if (array_key_exists('read_write_timeout', $options)) {
            $configuration['client']['read_write_timeout'] = filter_var($options['read_write_timeout'], FILTER_VALIDATE_FLOAT);
        }

        if (array_key_exists('tcp_nodelay', $options)) {
            $configuration['client']['tcp_nodelay'] = filter_var($options['tcp_nodelay'], FILTER_VALIDATE_BOOLEAN);
        }

        if (array_key_exists('tls', $options)) {
            $configuration['client']['ssl'] = [];
            // See https://www.php.net/manual/en/context.ssl.php
            $allowed = [
                'peer_name',
                'verify_peer',
                'verify_peer_name',
                'allow_self_signed',
                'cafile',
                'capath',
                'local_cert',
                'local_pk',
                'passphrase',
                'verify_depth',
                'ciphers',
                'capture_peer_cert',
                'capture_peer_cert_chain',
                'SNI_enabled',
                'disable_compression',
                'peer_fingerprint',
                'security_level',
            ];
            foreach ($allowed as $key) {
                if (array_key_exists($key, $options['tls'])) {
                    $configuration['client']['ssl'][$key] = $options['tls'][$key];
                }
            }
        }

        // Build exchange configuration
        $configuration['exchange'] = [
            'name' => $options['exchange']['name'] ?? 'messenger',
            'type' => $options['exchange']['type'] ?? 'fanout',
            'passive' => filter_var($options['exchange']['passive'] ?? false, FILTER_VALIDATE_BOOLEAN),
            'durable' => filter_var($options['exchange']['durable'] ?? true, FILTER_VALIDATE_BOOLEAN),
            'auto_delete' => filter_var($options['exchange']['auto_delete'] ?? false, FILTER_VALIDATE_BOOLEAN),
            'default_publish_routing_key' => $options['exchange']['default_publish_routing_key'] ?? '',
            'arguments' => $options['exchange']['arguments'] ?? [],
        ];

        if (!in_array($configuration['exchange']['type'], ['direct', 'fanout', 'topic'])) {
            throw new InvalidArgumentException(sprintf('The given exchange type "%s" is invalid.', $configuration['exchange']['type']));
        }

        // Build queues configurations
        // When no queues are defined, auto-define one with the same name as the exchange
        if (empty($options['queues'])) {
            $options['queues'] = [
                $configuration['exchange']['name'] => [],
            ];
        }
        $configuration['queues'] = [];
        foreach ($options['queues'] as $queueName => $queueOptions) {
            if (!is_array($queueOptions)) {
                $queueOptions = [];
            }
            $configuration['queues'][$queueName] = [
                'passive' => filter_var($queueOptions['passive'] ?? false, FILTER_VALIDATE_BOOLEAN),
                'durable' => filter_var($queueOptions['durable'] ?? true, FILTER_VALIDATE_BOOLEAN),
                'exclusive' => filter_var($queueOptions['exclusive'] ?? false, FILTER_VALIDATE_BOOLEAN),
                'auto_delete' => filter_var($queueOptions['auto_delete'] ?? false, FILTER_VALIDATE_BOOLEAN),
                // There is no "binding without a routing key", it is actually an empty string RK in such case
                'binding_keys' => $queueOptions['binding_keys'] ?? [''],
                'binding_arguments' => $queueOptions['binding_arguments'] ?? [],
            ];

            // Some arguments must be integers. Cast them in case they were defined via DSN
            $arguments = $queueOptions['arguments'] ?? [];
            foreach (self::QUEUE_INTEGER_ARGUMENTS as $key) {
                if (!array_key_exists($key, $arguments)) {
                    continue;
                }

                if (!is_numeric($arguments[$key])) {
                    throw new InvalidArgumentException(sprintf('Integer expected for queue argument "%s", "%s" given.', $key, get_debug_type($arguments[$key])));
                }

                $arguments[$key] = (int) $arguments[$key];
            }
            $configuration['queues'][$queueName]['arguments'] = $arguments;
        }

        // Build delay configuration
        // Support `delay.exchange_name` and `delay.queue_name_pattern` for better compatibility with ext-based AMQP transport
        if (array_key_exists('exchange_name', $options['delay'] ?? [])) {
            $options['delay']['exchange']['name'] = $options['delay']['exchange_name'];
            unset($options['delay']['exchange_name']);
        }
        if (array_key_exists('queue_name_pattern', $options['delay'] ?? [])) {
            $options['delay']['queue_template']['name_pattern'] = $options['delay']['queue_name_pattern'];
        }
        $configuration['delay'] = [
            'queue_template' => [
                'name_pattern' => $options['delay']['queue_template']['name_pattern'] ?? 'delay_%exchange_name%_%routing_key%_%delay%',
                'passive' => filter_var($options['delay']['queue_template']['passive'] ?? false, FILTER_VALIDATE_BOOLEAN),
                'durable' => filter_var($options['delay']['queue_template']['durable'] ?? true, FILTER_VALIDATE_BOOLEAN),
                'auto_delete' => filter_var($options['delay']['queue_template']['auto_delete'] ?? false, FILTER_VALIDATE_BOOLEAN),
                'exclusive' => filter_var($options['delay']['queue_template']['exclusive'] ?? false, FILTER_VALIDATE_BOOLEAN),
                'arguments' => $options['delay']['queue_template']['arguments'] ?? [],
            ],
            'exchange' => [
                'name' => $options['delay']['exchange']['name'] ?? 'delays',
                'type' => $options['delay']['exchange']['type'] ?? 'direct',
                'passive' => filter_var($options['delay']['exchange']['passive'] ?? false, FILTER_VALIDATE_BOOLEAN),
                'durable' => filter_var($options['delay']['exchange']['durable'] ?? true, FILTER_VALIDATE_BOOLEAN),
                'auto_delete' => filter_var($options['delay']['exchange']['auto_delete'] ?? false, FILTER_VALIDATE_BOOLEAN),
                'arguments' => $options['delay']['exchange']['arguments'] ?? [],
            ],
        ];

        // Other options
        $configuration['prefetch_count'] = filter_var($options['prefetch_count'] ?? 0, FILTER_VALIDATE_INT);
        $this->autoSetup = $configuration['auto_setup'] = filter_var($options['auto_setup'] ?? true, FILTER_VALIDATE_BOOLEAN);
        $this->waitTime = $configuration['run_timeout'] = filter_var($options['run_timeout'] ?? 0.1, FILTER_VALIDATE_FLOAT);
        if ($this->waitTime <= 0.0) {
            throw new InvalidArgumentException(sprintf('Expected `run_timeout` to be a positive float, got %f.', $configuration['run_timeout']));
        }

        $this->configuration = $configuration;
        $this->factory = $factory ?? new BunnyFactory();
    }

    public static function fromDsn(string $dsn, array $options = [], ?BunnyFactory $factory = null): self
    {
        if (false === $components = parse_url($dsn)) {
            throw new InvalidArgumentException(sprintf('The given Bunny DSN "%s" is invalid.', $dsn));
        }

        if (0 === strpos($dsn, 'amqps+bunny://') xor array_key_exists('tls', $options)) {
            throw new InvalidArgumentException('TLS requires both "amqps+bunny://" protocol and TLS options.');
        }

        $path = isset($components['path']) ? explode('/', trim($components['path'], '/')) : [];
        parse_str($components['query'] ?? '', $query);

        $options = array_replace_recursive($options, $query);

        if (array_key_exists('host', $components)) {
            $options['host'] = $components['host'];
        }

        if (array_key_exists('port', $components)) {
            $options['port'] = $components['port'];
        }

        if (array_key_exists('user', $components)) {
            $options['user'] = $components['user'];
        }

        if (array_key_exists('pass', $components)) {
            $options['password'] = $components['pass'];
        }

        if (!empty($path[0])) {
            $options['vhost'] = urldecode($path[0]);
        }

        if (!empty($path[1])) {
            $options['exchange']['name'] = $path[1];
        }

        return new self($options, $factory);
    }

    public function getConfiguration(): array
    {
        return $this->configuration;
    }

    /**
     * @return string[]
     */
    public function getQueueNames(): array
    {
        return array_keys($this->configuration['queues']);
    }

    private function getClient(): Client
    {
        if (null === $this->client) {
            $this->client = $this->factory->createConnection($this->configuration['client']);
            // TODO: Re-throw when connection failed with credentials (w/o password).
            if (!$this->client->isConnected()) {
                $this->client->connect();
            }
        }

        return $this->client;
    }

    private function getChannel(): Channel
    {
        if (null === $this->channel) {
            $this->channel = $this->getClient()->channel();
            $this->channel->qos(0, $this->configuration['prefetch_count']);
        }

        return $this->channel;
    }

    private function getBuffer(): \SplQueue
    {
        if (null === $this->buffer) {
            $this->buffer = $this->factory->createBuffer();
        }

        return $this->buffer;
    }

    private function declareExchange()
    {
        $this->getChannel()->exchangeDeclare(
            $this->configuration['exchange']['name'],
            $this->configuration['exchange']['type'],
            $this->configuration['exchange']['passive'],
            $this->configuration['exchange']['durable'],
            $this->configuration['exchange']['auto_delete'],
            false,
            false,
            $this->configuration['exchange']['arguments']
        );
    }

    private function declareAndBindQueues()
    {
        $channel = $this->getChannel();

        foreach ($this->configuration['queues'] as $queueName => $queueConfig) {
            $channel->queueDeclare(
                $queueName,
                $queueConfig['passive'],
                $queueConfig['durable'],
                $queueConfig['exclusive'],
                $queueConfig['auto_delete'],
                false,
                $queueConfig['arguments']
            );

            $bindingKeys = $queueConfig['binding_keys'];
            foreach ($bindingKeys as $bindingKey) {
                $channel->queueBind(
                    $queueName,
                    $this->configuration['exchange']['name'],
                    $bindingKey,
                    false,
                    $queueConfig['binding_arguments']
                );
            }
        }
    }

    private function declareDelayExchange()
    {
        $this->getChannel()->exchangeDeclare(
            $this->configuration['delay']['exchange']['name'],
            $this->configuration['delay']['exchange']['type'],
            $this->configuration['delay']['exchange']['passive'],
            $this->configuration['delay']['exchange']['durable'],
            $this->configuration['delay']['exchange']['auto_delete'],
            false,
            false,
            $this->configuration['delay']['exchange']['arguments']
        );
    }

    private function declareDelayQueue(int $delay, string $routingKey, bool $isRetryAttempt)
    {
        $channel = $this->getChannel();
        $queueName = $this->getRoutingKeyForDelay($delay, $routingKey, $isRetryAttempt);
        $arguments = [
            'x-message-ttl' => $delay,
            // Auto-delete queue. If another messages is published into this queue, lease is renewed
            'x-expires' => $delay + 10000,
            // Message should be broadcasted to all consumers during delay, but to only one queue during retry.
            // With empty string as value, message is published into RabbitMQ's default exchange.
            // The default exchange is a direct exchange with no name (empty string) pre-declared by the broker.
            // It is implicitly bound to every queue, with a routing key equal to the queue name.
            'x-dead-letter-exchange' => $isRetryAttempt ? '' : $this->configuration['exchange']['name'],
            // Release from DLX with original routing key
            'x-dead-letter-routing-key' => $routingKey ?? '',
        ];

        $template = $this->configuration['delay']['queue_template'];
        $channel->queueDeclare(
            $queueName,
            $template['passive'],
            $template['durable'],
            $template['exclusive'],
            $template['auto_delete'],
            false,
            array_merge($template['arguments'], $arguments)
        );

        $channel->queueBind(
            $queueName,
            $this->configuration['delay']['exchange']['name'],
            $queueName
        );
    }

    public function setup(): void
    {
        $this->declareExchange();
        $this->declareAndBindQueues();
        $this->declareDelayExchange();
    }

    public function get(array $queueNames): iterable
    {
        if ($this->autoSetup) {
            $this->setup();
        }

        $client = $this->getClient();
        $channel = $this->getChannel();

        $queuesToAdd = array_diff($queueNames, array_keys($this->consumerTags));
        foreach ($queuesToAdd as $queueName) {
            $this->consumerTags[$queueName] = $channel->consume(
                function (BunnyMessage $message) use ($queueName) {
                    // We need this info in the receiver, and it's simpler than storing a tuple
                    $message->headers['__source_queue_name__'] = $queueName;
                    $this->getBuffer()->enqueue($message);
                },
                $queueName
            )->consumerTag;
        }

        $queuesToRemove = array_diff(array_keys($this->consumerTags), $queueNames);
        foreach ($queuesToRemove as $queueName) {
            $channel->cancel($this->consumerTags[$queueName]);
            unset($this->consumerTags[$queueName]);
        }

        $client->run($this->waitTime);

        $buffer = $this->getBuffer();
        while (!$buffer->isEmpty()) {
            yield $buffer->dequeue();
        }

        return null;
    }

    public function ack(BunnyMessage $message): void
    {
        $this->getChannel()->ack($message);
    }

    public function nack(BunnyMessage $message): void
    {
        $this->getChannel()->nack($message, false, false);
    }

    private function getRoutingKeyForDelay(int $delay, ?string $finalRoutingKey, bool $isRetryAttempt): string
    {
        $action = $isRetryAttempt ? '_retry' : '_delay';

        return str_replace(
            ['%delay%', '%exchange_name%', '%routing_key%'],
            [$delay, $this->configuration['exchange']['name'], $finalRoutingKey ?? ''],
            $this->configuration['delay']['queue_template']['name_pattern']
        ).$action;
    }

    private function getRoutingKeyForMessage(?BunnyStamp $bunnyStamp): ?string
    {
        return (null !== $bunnyStamp ? $bunnyStamp->getRoutingKey() : null) ?? $this->configuration['exchange']['default_publish_routing_key'];
    }

    private function publishOnExchange(string $exchangeName, string $body, string $routingKey, array $headers = [], ?BunnyStamp $bunnyStamp = null): void
    {
        $attributes = $bunnyStamp ? $bunnyStamp->getAttributes() : [];
        $attributes = array_merge($attributes, $headers);
        $attributes['delivery-mode'] = $attributes['delivery-mode'] ?? 2;
        $attributes['timestamp'] = $attributes['timestamp'] ?? new \DateTimeImmutable();

        $this->getChannel()->publish(
            $body,
            $attributes,
            $exchangeName,
            $routingKey
        );
    }

    private function publishWithDelay(string $body, array $headers, int $delay = 0, ?BunnyStamp $bunnyStamp = null): void
    {
        $routingKey = $this->getRoutingKeyForMessage($bunnyStamp);
        $isRetryAttempt = $bunnyStamp ? $bunnyStamp->isRetryAttempt() : false;

        $this->declareDelayQueue($delay, $routingKey, $isRetryAttempt);

        $this->publishOnExchange(
            $this->configuration['delay']['exchange']['name'],
            $body,
            $this->getRoutingKeyForDelay($delay, $routingKey, $isRetryAttempt),
            $headers,
            $bunnyStamp
        );
    }

    public function publish(string $body, array $headers, int $delay = 0, ?BunnyStamp $bunnyStamp = null): void
    {
        if ($this->autoSetup) {
            $this->setup();
        }

        if (0 !== $delay) {
            $this->publishWithDelay($body, $headers, $delay, $bunnyStamp);

            return;
        }

        $this->publishOnExchange(
            $this->configuration['exchange']['name'],
            $body,
            $this->getRoutingKeyForMessage($bunnyStamp),
            $headers,
            $bunnyStamp
        );
    }

    public function countMessagesInQueues(): int
    {
        if ($this->autoSetup) {
            $this->setup();
        }

        $channel = $this->getChannel();

        return array_sum(array_map(function ($queueName) use ($channel) {
            return $channel->queueDeclare($queueName, true)->messageCount;
        }, $this->getQueueNames()));
    }
}
