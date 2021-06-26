<?php

declare(strict_types=1);

namespace Symfony\Component\Messenger\Bridge\Bunny\Tests\Transport;

use Bunny\Message;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Messenger\Bridge\Bunny\Transport\BunnyStamp;

class BunnyStampTest extends TestCase
{
    private function generateAttribtues(): array
    {
        return [
            'delivery-mode' => 2,
            'priority' => 5,
            'type' => 'App\MyEvent',
            'X-Message-Stamp-Symfony\Component\Messenger\Stamp\BusNameStamp' => '[{"busName":"command.bus"}]'
        ];
    }

    private function generatePreviousStampAttributes(): array
    {
        return [
            'priority' => 8,
            'app-id' => 'my-app',
        ];
    }

    public function testItCreatesStampFromMessage()
    {
        $attributes = $this->generateAttribtues();
        $bunnyMessage = new Message('', '', false, '', '', $attributes, '');

        $stamp = BunnyStamp::createFromBunnyMessage($bunnyMessage);
        $this->assertEquals($attributes, $stamp->getAttributes());
        $this->assertFalse($stamp->isRetryAttempt());
    }

    public function testItCreatesStampFromMessageWithPreviousStampAttributesAndRoutingKey()
    {
        $attributes = $this->generateAttribtues();
        $bunnyMessage = new Message('', '', false, '', '', $attributes, '');
        $previousAttributes = $this->generatePreviousStampAttributes();
        $previousStamp = new BunnyStamp('my_key', $previousAttributes);

        $resultingAttributes = array_merge($attributes, $previousAttributes);

        $stamp = BunnyStamp::createFromBunnyMessage($bunnyMessage, $previousStamp);
        $this->assertEquals($resultingAttributes, $stamp->getAttributes());
        $this->assertFalse($stamp->isRetryAttempt());
        $this->assertEquals('my_key', $stamp->getRoutingKey());
    }

    public function testItRecognizesRetryByRetryRoutingKeyPresence()
    {
        $attributes = $this->generateAttribtues();
        $bunnyMessage = new Message('', '', false, '', '', $attributes, '');

        $stamp = BunnyStamp::createFromBunnyMessage($bunnyMessage, null, 'my_key');
        $this->assertTrue($stamp->isRetryAttempt());
        $this->assertEquals('my_key', $stamp->getRoutingKey());
    }

    public function testItCreatesWithAttributes()
    {
        $attributes = $this->generateAttribtues();

        $stamp = BunnyStamp::createWithAttributes($attributes);
        $this->assertEquals($attributes, $stamp->getAttributes());
    }

    public function testItCreatesWithAttributesWithPreviousStampAttributesAndRoutingKey()
    {
        $attributes = $this->generateAttribtues();
        $previousAttributes = $this->generatePreviousStampAttributes();
        $previousStamp = new BunnyStamp('my_key', $previousAttributes);

        $resultingAttributes = array_merge($previousAttributes, $attributes);

        $stamp = BunnyStamp::createWithAttributes($attributes, $previousStamp);
        $this->assertEquals($resultingAttributes, $stamp->getAttributes());
        $this->assertEquals('my_key', $stamp->getRoutingKey());
        $this->assertFalse($stamp->isRetryAttempt());
    }
}
