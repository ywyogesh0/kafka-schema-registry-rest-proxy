package com.examples.avro;

public class Constants {

    // Common
    public static final String BOOTSTRAP_SERVERS = "bootstrap.servers";
    public static final String TOPIC_NAME = "customer-avro";
    public static final String SCHEMA_REGISTRY_URL =
            "schema.registry.url"; // confluent schema registry url to register schemas via producer

    // Producer
    public static final String ACKS =
            "acks"; // acknowledgement b/w - producer and (leader followed by followers : in-sync replicas)

    public static final String RETRIES =
            "retries"; // retries sending unacknowledged request (relation - max.in.flight.requests.per.connection)

    public static final String MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION =
            "max.in.flight.requests.per.connection"; // count = 1,  guarantees ordering of records
    // the max number of unacknowledged request client sends per connection before blocking

    public static final String KEY_SERIALIZER =
            "key.serializer"; // name of the serializer class for the key

    public static final String VALUE_SERIALIZER =
            "value.serializer"; // name of the serializer class for the value

    // Consumer
    public static final String GROUP_ID =
            "group.id"; // consumer belongs to respective consumer group id : off-set management strategy

    public static final String ENABLE_AUTO_COMMIT =
            "enable.auto.commit"; // automatically commit off-sets periodically by consumer

    public static final String AUTO_OFFSET_RESET =
            "auto.offset.reset"; // what to do when don't have any off-set

    public static final String KEY_DESERIALIZER =
            "key.deserializer"; // name of the deserializer class for the key

    public static final String VALUE_DESERIALIZER =
            "value.deserializer"; // name of the deserializer class for the value

    public static final String SPECIFIC_AVRO_READER =
            "specific.avro.reader"; // boolean flag to read specific avro record
}
