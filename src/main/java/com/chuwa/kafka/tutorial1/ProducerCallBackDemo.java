package com.chuwa.kafka.tutorial1;


import org.slf4j.Logger;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * @author b1go
 * @date 9/25/22 4:41 PM
 */
public class ProducerCallBackDemo {

    public static void main(String[] args) {
        final Logger logger = LoggerFactory.getLogger(ProducerCallBackDemo.class);
        
        String bootstrapServers = "127.0.0.1:9092";

        // 1. create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 2. create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        // 3. create a producer record
        for (int i = 0; i < 10; i++) {
            String topic = "first_topic";
            String value = "Kafka By The Sea " + Integer.toString(i);
            String key = "id_" + Integer.toString(i);

            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);

            logger.info("Key: " + key);

            // 4. send data - asynchronous
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    // executes everytime a record is successfully sent or an exception is thrown
                    if (e == null) {
                        logger.info("Received new metadata. \n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp()
                        );
                    } else {
                        logger.info("Error while producing", e);
                    }
                }
            });
        }

        // 5. flush data
        producer.flush();

        // 6. close producer
        producer.close();
    }
}
