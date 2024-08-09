<?php

namespace App\Http\Controllers;

use Illuminate\Http\Request;
use Junges\Kafka\Facades\Kafka;
use Junges\Kafka\Message\Message;

class KafkaController extends Controller
{
    public function sendMessage(Request $request)
    {
        $kafkaMessage = new Message(
            body: 'message',
        );

        Kafka::publish('kafka:9092')
            ->onTopic('topic')
            ->withMessage($kafkaMessage)
            ->send();

        return response()->json(['status' => 'Message sent']);
    }

    public function readMessage()
    {
        $consumer = Kafka::consumer(['topic'], 'laravel-group', 'kafka:9092');

        $consumer->assignPartitions([
            new \RdKafka\TopicPartition('topic', 0)
        ]);

        $message = $consumer->withHandler(function(\Junges\Kafka\Contracts\ConsumerMessage $message, \Junges\Kafka\Contracts\MessageConsumer $consumer){
            return $message->getBody();
        });

        return response()->json(['status' => 'Message received', 'messages' => $message]);
    }
}
