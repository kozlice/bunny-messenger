<?php

require_once 'vendor/autoload.php';

$dsn = 'amqp+bunny://skyeng_user:skyeng_user@localhost:10030';
$connection = \Symfony\Component\Messenger\Bridge\Bunny\Transport\Connection::fromDsn($dsn);

$messages = $connection->get(['test.primary', 'test.secondary']);
foreach ($messages as $message) {
    dump($message);
    $connection->ack($message);
}

sleep(10);

dump('-----------------------------------------------------------------------------');

$messages = $connection->get(['test.primary']);
foreach ($messages as $message) {
    dump($message);
    $connection->ack($message);
}
