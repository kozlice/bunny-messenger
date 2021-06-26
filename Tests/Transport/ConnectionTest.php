<?php

declare(strict_types=1);

namespace Symfony\Component\Messenger\Bridge\Bunny\Tests\Transport;

use Bunny\Channel;
use Bunny\Client;
use Bunny\Message as BunnyMessage;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Messenger\Bridge\Bunny\Transport\BunnyFactory;
use Symfony\Component\Messenger\Bridge\Bunny\Transport\BunnyStamp;
use Symfony\Component\Messenger\Bridge\Bunny\Transport\Connection;
use Symfony\Component\Messenger\Exception\InvalidArgumentException;

/**
 * @property Client|MockObject  client
 * @property Channel|MockObject channel
 * @property TestFactory        factory
 * @property \SplQueue          buffer
 */
class ConnectionTest extends TestCase
{
    protected function setUp(): void
    {
        $this->client = $this->createMock(Client::class);
        $this->channel = $this->createMock(Channel::class);
        $this->client->expects($this->any())->method('channel')->willReturn($this->channel);
        $this->buffer = new \SplQueue();
        $this->factory = new TestFactory(
            $this->client,
            $this->buffer
        );
    }

    public function testItThrowsOnInvalidDsn()
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('The given Bunny DSN');

        Connection::fromDsn('amqp+bunny://:*_', []);
    }

    public function testItBuildsConfigurationAndDsnHasPriority()
    {
        $connection = Connection::fromDsn(
            'amqp+bunny://somebody:somewhat@somewhere:10000/hello/different-exchange-name'.
            '?prefetch_count=10'.
            '&heartbeat=60'.
            '&connection_timeout=3'.
            '&read_write_timeout=3'.
            '&tcp_nodelay=1'.
            '&exchange[durable]=false'.
            '&exchange[default_publish_routing_key]=some-rk'.
            '&delay[queue_name_pattern]=some_pattern'.
            '&delay[exchange_name]=different-delay-exchange-name',
            [
                'delay' => [
                    'exchange' => [
                        'durable' => false,
                    ],
                ],
                'exchange' => [
                    'arguments' => ['alternate-exchange' => 'my-ae'],
                ],
                'run_timeout' => 1.0,
            ],
            $this->factory
        );

        $configuration = $connection->getConfiguration();
        $this->assertEqualsWithDelta(1.0, $configuration['run_timeout'], 0.001);
        $this->assertEqualsWithDelta(60.0, $configuration['client']['heartbeat'], 0.001);
        $this->assertEqualsWithDelta(3.0, $configuration['client']['timeout'], 0.001);
        $this->assertEqualsWithDelta(3.0, $configuration['client']['read_write_timeout'], 0.001);
        unset(
            $configuration['run_timeout'],
            $configuration['client']['heartbeat'],
            $configuration['client']['timeout'],
            $configuration['client']['read_write_timeout']
        );
        $this->assertEquals([
            'client' => [
                'host' => 'somewhere',
                'port' => 10000,
                'user' => 'somebody',
                'password' => 'somewhat',
                'vhost' => 'hello',
                'tcp_nodelay' => true,
            ],
            'exchange' => [
                'name' => 'different-exchange-name',
                'type' => 'fanout',
                'passive' => false,
                'durable' => false,
                'auto_delete' => false,
                'arguments' => ['alternate-exchange' => 'my-ae'],
                'default_publish_routing_key' => 'some-rk',
            ],
            'queues' => [
                'different-exchange-name' => [
                    'passive' => false,
                    'durable' => true,
                    'auto_delete' => false,
                    'exclusive' => false,
                    'arguments' => [],
                    'binding_keys' => [''],
                    'binding_arguments' => [],
                ],
            ],
            'delay' => [
                'queue_template' => [
                    'name_pattern' => 'some_pattern',
                    'passive' => false,
                    'durable' => true,
                    'auto_delete' => false,
                    'exclusive' => false,
                    'arguments' => [],
                ],
                'exchange' => [
                    'name' => 'different-delay-exchange-name',
                    'type' => 'direct',
                    'passive' => false,
                    'durable' => false,
                    'auto_delete' => false,
                    'arguments' => [],
                ],
            ],
            'prefetch_count' => 10,
            'auto_setup' => true,
        ], $configuration);
    }

    public function testItBuildsConfigurationWithTls()
    {
        $connection = Connection::fromDsn(
            'amqps+bunny://localhost'.
            '?tls[verify_peer]=true'.
            '&tls[verify_peer_name]=true'.
            '&tls[verify_depth]=1'.
            '&tls[peer_fingerprint][]=hash_1'.
            '&tls[peer_fingerprint][]=hash_2'.
            '&tls[disable_compression]=true',
            ['tls' => [
                'peer_name' => 'my_peer_name',
                'allow_self_signed' => 1,
                'capath' => '/etc/ssl/certs',
                'cafile' => 'cert.pem',
                'local_cert' => 'cert.crt',
                'local_pk' => 'cert.key',
                'passphrase' => 'password',
                'ciphers' => 'my_ciphers',
                'SNI_enabled' => 0,
                'security_level' => 1,
                'capture_peer_cert' => true,
                'capture_peer_cert_chain' => true,
            ]]
        );
        $configuration = $connection->getConfiguration();
        $this->assertEquals([
            'peer_name' => 'my_peer_name',
            'verify_peer' => true,
            'verify_peer_name' => true,
            'allow_self_signed' => true,
            'capath' => '/etc/ssl/certs',
            'cafile' => 'cert.pem',
            'local_cert' => 'cert.crt',
            'local_pk' => 'cert.key',
            'passphrase' => 'password',
            'verify_depth' => 1,
            'ciphers' => 'my_ciphers',
            'capture_peer_cert' => true,
            'capture_peer_cert_chain' => true,
            'SNI_enabled' => false,
            'disable_compression' => true,
            'peer_fingerprint' => ['hash_1', 'hash_2'],
            'security_level' => 1,
        ], $configuration['client']['ssl']);
    }

    public function invalidTlsConfigDataProvider(): iterable
    {
        return [
            ['amqp+bunny://localhost/%2f/messages', ['tls' => []]],
            ['amqps+bunny://localhost/%2f/messages', []],
        ];
    }

    /**
     * @dataProvider invalidTlsConfigDataProvider
     */
    public function testItThrowsOnInvalidTlsConfig(string $dsn, array $options)
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('TLS requires both "amqps+bunny://" protocol and TLS options.');

        Connection::fromDsn($dsn, $options);
    }

    public function invalidQueueArgumentsDataProvider(): iterable
    {
        $baseDsn = 'amqp+bunny://localhost/%2f/messages';

        return [
            [$baseDsn.'?queues[messages][arguments][x-delay]=not-a-number', []],
            [$baseDsn.'?queues[messages][arguments][x-expires]=not-a-number', []],
            [$baseDsn.'?queues[messages][arguments][x-max-length]=not-a-number', []],
            [$baseDsn.'?queues[messages][arguments][x-max-length-bytes]=not-a-number', []],
            [$baseDsn.'?queues[messages][arguments][x-max-priority]=not-a-number', []],
            [$baseDsn.'?queues[messages][arguments][x-message-ttl]=not-a-number', []],

            // Ensure the exception is thrown when the arguments are passed via the array options
            [$baseDsn, ['queues' => ['messages' => ['arguments' => ['x-delay' => 'not-a-number']]]]],
            [$baseDsn, ['queues' => ['messages' => ['arguments' => ['x-expires' => 'not-a-number']]]]],
            [$baseDsn, ['queues' => ['messages' => ['arguments' => ['x-max-length' => 'not-a-number']]]]],
            [$baseDsn, ['queues' => ['messages' => ['arguments' => ['x-max-length-bytes' => 'not-a-number']]]]],
            [$baseDsn, ['queues' => ['messages' => ['arguments' => ['x-max-priority' => 'not-a-number']]]]],
            [$baseDsn, ['queues' => ['messages' => ['arguments' => ['x-message-ttl' => 'not-a-number']]]]],
        ];
    }

    /**
     * @dataProvider invalidQueueArgumentsDataProvider
     */
    public function testItThrowsOnInvalidQueueArguments(string $dsn, array $options)
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Integer expected for queue argument');

        Connection::fromDsn($dsn, $options);
    }

    public function invalidExchangeTypeDataProvider(): iterable
    {
        $baseDsn = 'amqp+bunny://localhost/%2f/messages';

        return [
            [$baseDsn.'?exchange[type]=invalid', []],
            [$baseDsn, ['exchange' => ['type' => 'invalid']]],
        ];
    }

    /**
     * @dataProvider invalidExchangeTypeDataProvider
     */
    public function testItThrowsOnInvalidExchangeType(string $dsn, array $options)
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('The given exchange type');

        Connection::fromDsn($dsn, $options);
    }

    public function invalidWaitTimeDataProvider(): iterable
    {
        $baseDsn = 'amqp+bunny://localhost/%2f/messages';

        return [
            [$baseDsn.'?run_timeout=0.0', []],
            [$baseDsn.'?run_timeout=-1', []],
            [$baseDsn, ['run_timeout' => 0]],
            [$baseDsn, ['run_timeout' => -1]],
        ];
    }

    /**
     * @dataProvider invalidWaitTimeDataProvider
     */
    public function testItThrowsOnInvalidRunTimeout(string $dsn, array $options)
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('to be a positive float');

        Connection::fromDsn($dsn, $options);
    }

    public function testItGetsQueueNamesFromConfiguration()
    {
        $connection = Connection::fromDsn('amqp+bunny://localhost', [
            'queues' => [
                'queue_0' => null,
                'queue_1' => null,
            ],
        ]);
        $this->assertEquals(['queue_0', 'queue_1'], $connection->getQueueNames());
    }

    public function testItUsesRunTimeoutFromConfiguration()
    {
        $this->channel
            ->expects($this->once())
            ->method('consume')
            ->willReturn((object) ['consumerTag' => 'amq.ctag-test-0'])
        ;

        $this->client
            ->expects($this->once())
            ->method('run')
            ->with($this->callback(function ($waitTime) {
                $this->assertEqualsWithDelta(1.0, $waitTime, 0.001);

                return true;
            }))
        ;

        $connection = Connection::fromDsn('amqp+bunny://localhost', ['run_timeout' => 1.0], $this->factory);
        $connection->get(['queue_0'])->current();
    }

    public function testItSetsPrefetchCount()
    {
        $this->channel
            ->expects($this->once())
            ->method('qos')
            ->with(0, 10)
        ;

        $connection = Connection::fromDsn('amqp+bunny://localhost', ['prefetch_count' => 10], $this->factory);
        $connection->setup();
    }

    public function testItControlsSubscriptionToQueues()
    {
        $this->channel
            ->expects($this->exactly(3))
            ->method('consume')
            ->withConsecutive(
                [$this->callback(function ($arg) { $this->assertIsCallable($arg); return true; }), 'queue_0'],
                [$this->callback(function ($arg) { $this->assertIsCallable($arg); return true; }), 'queue_1'],
                [$this->callback(function ($arg) { $this->assertIsCallable($arg); return true; }), 'queue_2']
            )->willReturnOnConsecutiveCalls(
                (object) ['consumerTag' => 'amq.ctag-test-0'],
                (object) ['consumerTag' => 'amq.ctag-test-1'],
                (object) ['consumerTag' => 'amq.ctag-test-2']
            )
        ;

        $this->channel
            ->expects($this->once())
            ->method('cancel')
            ->with('amq.ctag-test-1')
        ;

        $connection = Connection::fromDsn('amqp+bunny://localhost', [], $this->factory);
        $connection->get(['queue_0', 'queue_1'])->current();
        $connection->get(['queue_0', 'queue_2'])->current();
    }

    public function testItYieldsMessagesFromBuffer()
    {
        $this->channel
            ->expects($this->once())
            ->method('consume')
            ->with(
                $this->callback(function ($func) {
                    for ($i = 1; $i <= 3; $i++) {
                        $message = new BunnyMessage('', $i, false, 'exchange_0', '', [], '{}');
                        $func($message);
                    }
                    return true;
                }),
                'queue_0'
            )
            ->willReturn((object) ['consumerTag' => 'amq.ctag-test-0'])
        ;

        $connection = Connection::fromDsn('amqp+bunny://localhost', [], $this->factory);
        $messages = iterator_to_array($connection->get(['queue_0']));
        $this->assertCount(3, $messages);
        $this->assertContainsOnlyInstancesOf(BunnyMessage::class, $messages);
    }

    public function testItAcknowledgesMessage()
    {
        $bunnyMessage = new BunnyMessage('', 1, false, 'test', '', [], '{}');
        $this->channel
            ->expects($this->once())
            ->method('ack')
            ->with($bunnyMessage)
        ;

        $connection = Connection::fromDsn('amqp+bunny://localhost', [], $this->factory);
        $connection->ack($bunnyMessage);
    }

    public function testItRejectsMessage()
    {
        $bunnyMessage = new BunnyMessage('', 1, false, 'test', '', [], '{}');
        $this->channel
            ->expects($this->once())
            ->method('nack')
            ->with($bunnyMessage, false, false)
        ;

        $connection = Connection::fromDsn('amqp+bunny://localhost', [], $this->factory);
        $connection->nack($bunnyMessage);
    }

    public function testItDeclaresExchangesAndQueuesInSetup()
    {
        $this->channel
            ->expects($this->exactly(2))
            ->method('exchangeDeclare')
            ->withConsecutive(
                ['messenger', 'fanout', false, true, false, false, false, ['alternate-exchange' => 'my-ae']],
                ['messenger_delays', 'direct', false, true, false, false, false, []]
            )
        ;

        $this->channel
            ->expects($this->exactly(2))
            ->method('queueDeclare')
            ->withConsecutive(
                ['queue_0', false, true, false, false, false, ['x-max-priority' => 255, 'x-overflow' => 'reject-publish']],
                ['queue_1', false, true, false, false, false, []]
            )
        ;

        $this->channel
            ->expects($this->exactly(3))
            ->method('queueBind')
            ->withConsecutive(
                ['queue_0', 'messenger', '', false, []],
                ['queue_1', 'messenger', 'my_key_0', false, ['x-match' => 'any']],
                ['queue_1', 'messenger', 'my_key_1', false, ['x-match' => 'any']]
            )
        ;

        $connection = Connection::fromDsn('amqp+bunny://localhost', [
            'exchange' => [
                'arguments' => ['alternate-exchange' => 'my-ae'],
            ],
            'queues' => [
                'queue_0' => [
                    'arguments' => [
                        'x-max-priority' => 255,
                        'x-overflow' => 'reject-publish',
                    ],
                ],
                'queue_1' => [
                    'binding_keys' => ['my_key_0', 'my_key_1'],
                    'binding_arguments' => ['x-match' => 'any'],
                ],
            ],
            'delay' => [
                'exchange_name' => 'messenger_delays',
            ],
        ], $this->factory);
        $connection->setup();
    }

    public function testItPublishesWithoutDelay()
    {
        $this->channel
            ->expects($this->once())
            ->method('publish')
            ->with(
                '{}',
                $this->callback(function ($attributes) {
                    $this->assertEquals(2, $attributes['delivery-mode']);
                    $this->assertInstanceOf(\DateTimeImmutable::class, $attributes['timestamp']);

                    return true;
                }),
                'messenger',
                ''
            )
        ;

        $connection = Connection::fromDsn('amqp+bunny://localhost', [], $this->factory);
        $connection->publish('{}', [], 0, null);
    }

    public function testItPublishesWithoutDelayWithDefaultRoutingKey()
    {
        $this->channel
            ->expects($this->once())
            ->method('publish')
            ->with(
                '{}',
                $this->callback(function ($attributes) {
                    $this->assertEquals(2, $attributes['delivery-mode']);
                    $this->assertInstanceOf(\DateTimeImmutable::class, $attributes['timestamp']);

                    return true;
                }),
                'messenger',
                'my-rk'
            )
        ;

        $connection = Connection::fromDsn('amqp+bunny://localhost', [
            'auto_setup' => false,
            'exchange' => ['default_publish_routing_key' => 'my-rk']
        ], $this->factory);
        $connection->publish('{}', [], 0, null);
    }

    public function testItPublishesWithoutDelayWithRoutingKeyAndAttributesFromStamp()
    {
        $this->channel
            ->expects($this->once())
            ->method('publish')
            ->with(
                '{}',
                $this->callback(function ($attributes) {
                    $this->assertEquals(255, $attributes['priority']);
                    $this->assertEquals(2, $attributes['delivery-mode']);
                    $this->assertInstanceOf(\DateTimeImmutable::class, $attributes['timestamp']);

                    return true;
                }),
                'messenger',
                'my-rk'
            )
        ;

        $connection = Connection::fromDsn('amqp+bunny://localhost', ['auto_setup' => false], $this->factory);
        $stamp = new BunnyStamp('my-rk', ['priority' => 255]);
        $connection->publish('{}', [], 0, $stamp);
    }

    public function testItPublishesWithDelay()
    {
        $this->channel
            ->expects($this->once())
            ->method('queueDeclare')
            ->with(
                'delay_messenger__5000_delay',
                false,
                true,
                false,
                false,
                false,
                [
                    'x-message-ttl' => 5000,
                    'x-expires' => 5000 + 10000,
                    'x-dead-letter-exchange' => 'messenger',
                    'x-dead-letter-routing-key' => '',
                ]
            )
        ;

        $this->channel
            ->expects($this->once())
            ->method('publish')
            ->with(
                '{}',
                $this->callback(function ($attributes) {
                    $this->assertEquals(2, $attributes['delivery-mode']);
                    $this->assertInstanceOf(\DateTimeImmutable::class, $attributes['timestamp']);

                    return true;
                }),
                'delays',
                'delay_messenger__5000_delay'
            )
        ;

        $connection = Connection::fromDsn('amqp+bunny://localhost', ['auto_setup' => false], $this->factory);
        $connection->publish('{}', [], 5000, null);
    }

    public function testItPublishesWithDelayForRetry()
    {
        $this->channel
            ->expects($this->once())
            ->method('queueDeclare')
            ->with(
                'delay_messenger_queue_0_5000_retry',
                false, true, false, false, false,
                [
                    'x-message-ttl' => 5000,
                    'x-expires' => 5000 + 10000,
                    'x-dead-letter-exchange' => '',
                    'x-dead-letter-routing-key' => 'queue_0',
                ]
            )
        ;

        $this->channel
            ->expects($this->once())
            ->method('publish')
            ->with(
                '{}',
                $this->callback(function ($attributes) {
                    $this->assertEquals(2, $attributes['delivery-mode']);
                    $this->assertInstanceOf(\DateTimeImmutable::class, $attributes['timestamp']);

                    return true;
                }),
                'delays',
                'delay_messenger_queue_0_5000_retry'
            )
        ;

        $stamp = BunnyStamp::createFromBunnyMessage(
            new BunnyMessage('', 1, false, '', '', [], ''),
            null,
            'queue_0'
        );
        $connection = Connection::fromDsn('amqp+bunny://localhost', ['auto_setup' => false], $this->factory);
        $connection->publish('{}', [], 5000, $stamp);
    }

    public function testItCountsMessagesInQueues()
    {
        $this->channel
            ->expects($this->exactly(2))
            ->method('queueDeclare')
            ->withConsecutive(
                ['queue_0', true],
                ['queue_1', true]
            )
            ->willReturnOnConsecutiveCalls(
                (object) ['messageCount' => 10],
                (object) ['messageCount' => 25]
            )
        ;

        $connection = Connection::fromDsn('amqp+bunny://localhost', [
            'auto_setup' => false,
            'queues' => [
                'queue_0' => null,
                'queue_1' => null,
            ],
        ], $this->factory);
        $this->assertEquals(35, $connection->countMessagesInQueues());
    }

    public function testItRunsAutoSetupForGet()
    {
        $this->channel->expects($this->atLeastOnce())->method('exchangeDeclare');
        $this->channel->expects($this->atLeastOnce())->method('queueDeclare');
        $this->channel->expects($this->atLeastOnce())->method('queueBind');
        $this->channel->expects($this->atLeastOnce())->method('consume')->willReturn((object) ['consumerTag' => 'amq.ctag-test']);

        $connection = Connection::fromDsn('amqp+bunny://localhost', [], $this->factory);
        $connection->get(['queue_0'])->current();
    }

    public function testItRunsAutoSetupForPublish()
    {
        $this->channel->expects($this->atLeastOnce())->method('exchangeDeclare');
        $this->channel->expects($this->atLeastOnce())->method('queueDeclare');
        $this->channel->expects($this->atLeastOnce())->method('queueBind');

        $connection = Connection::fromDsn('amqp+bunny://localhost', [], $this->factory);
        $connection->publish('{}', []);
    }

    public function testItRunsAutoSetupForCountMessagesInQueues()
    {
        $this->channel->expects($this->atLeastOnce())->method('exchangeDeclare');
        $this->channel->expects($this->atLeastOnce())->method('queueDeclare')->willReturn((object) ['messageCount' => 0]);
        $this->channel->expects($this->atLeastOnce())->method('queueBind');

        $connection = Connection::fromDsn('amqp+bunny://localhost', [], $this->factory);
        $connection->countMessagesInQueues();
    }
}

class TestFactory extends BunnyFactory
{
    private $client;
    private $buffer;

    public function __construct(Client $client, \SplQueue $buffer = null)
    {
        $this->client = $client;
        $this->buffer = $buffer ?? new \SplQueue();
    }

    public function createConnection(array $config): Client
    {
        return $this->client;
    }

    public function createBuffer(): \SplQueue
    {
        return $this->buffer;
    }
}
