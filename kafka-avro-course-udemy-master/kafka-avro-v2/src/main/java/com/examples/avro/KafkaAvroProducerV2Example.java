package com.examples.avro;

import com.example.Customer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

import static com.examples.avro.Constants.*;

public class KafkaAvroProducerV2Example {

    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.setProperty(BOOTSTRAP_SERVERS, "localhost:9092");
        properties.setProperty(ACKS, "all");
        properties.setProperty(RETRIES, "10");
        properties.setProperty(MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1");
        properties.setProperty(KEY_SERIALIZER, StringSerializer.class.getName());
        properties.setProperty(VALUE_SERIALIZER, KafkaAvroSerializer.class.getName());
        properties.setProperty(SCHEMA_REGISTRY_URL, "http://localhost:8081");

        Producer<String, Customer> producer = new KafkaProducer<String, Customer>(properties);

        Customer customer = Customer.newBuilder()
                .setAge(29)
                .setFirstName("Shyam")
                .setLastName("Walia")
                .setHeight(5.11f)
                .setWeight(95.5f)
                .setPhoneNumber("9910805482")
                .setEmail("shyam@gmail.com")
                .build();

        ProducerRecord<String, Customer> producerRecord = new ProducerRecord<>(TOPIC_NAME, customer);

        producer.send(producerRecord, (recordMetadata, exception) -> {
                    if (exception == null) {
                        System.out.println("RecordMetadata : " + recordMetadata.toString());
                    } else {
                        System.out.println(exception.getMessage());
                    }
                }
        );

        producer.flush();
        producer.close();
    }
}
