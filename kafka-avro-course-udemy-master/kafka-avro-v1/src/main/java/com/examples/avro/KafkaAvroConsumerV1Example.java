package com.examples.avro;

import com.example.Customer;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.Properties;

import static com.examples.avro.Constants.*;

public class KafkaAvroConsumerV1Example {

    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.setProperty(BOOTSTRAP_SERVERS, "localhost:9092");
        properties.setProperty(GROUP_ID, "customer-consumer-group-v1");
        properties.setProperty(ENABLE_AUTO_COMMIT, "false");
        properties.setProperty(AUTO_OFFSET_RESET, "earliest");
        properties.setProperty(KEY_DESERIALIZER, StringDeserializer.class.getName());
        properties.setProperty(VALUE_DESERIALIZER, KafkaAvroDeserializer.class.getName());
        properties.setProperty(SCHEMA_REGISTRY_URL, "http://localhost:8081");
        properties.setProperty(SPECIFIC_AVRO_READER, "true");

        Consumer<String, Customer> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singleton(TOPIC_NAME));

        ConsumerRecords<String, Customer> consumerRecords;

        System.out.println("Waiting for Records...");

        while (true) {
            System.out.println("Polling...");
            consumerRecords = consumer.poll(5000);
            for (ConsumerRecord<String, Customer> consumerRecord : consumerRecords) {
                System.out.println("Topic = " + consumerRecord.topic());
                System.out.println("Offset = " + consumerRecord.offset());
                System.out.println("Partition = " + consumerRecord.partition());
                System.out.println();
                System.out.println("Customer Key = " + consumerRecord.key());
                System.out.println("Customer Value = " + consumerRecord.value());
            }

            consumer.commitSync();
        }
    }
}
