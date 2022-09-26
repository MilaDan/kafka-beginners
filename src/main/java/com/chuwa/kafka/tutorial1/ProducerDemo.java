package com.chuwa.kafka.tutorial1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * @author b1go
 * @date 9/25/22 4:41 PM
 */
public class ProducerDemo {

    public static void main(String[] args) {
        String bootstrapServers = "127.0.0.1:9092";

        // 1. create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 2. create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        // 3. create a producer record
        ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "Kafka By The Sea");

        // 4. send data - asynchronous
        producer.send(record);

        // 5. flush data
        producer.flush();

        // 6. close producer
        producer.close();
    }
}
