package com.example.abhi.kafka;

import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemoWithKeys {
    public static void main(String[] args) throws InterruptedException, ExecutionException {

        Logger logger = LoggerFactory.getLogger(ProducerDemoWithKeys.class);

        String bootstrapServer = "127.0.0.1:9092";

        // Create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int i=0; i<10; i++) {
            // Create a producer record
            String topic = "first_topic";
            String value = "Hello world " + Integer.toString(i);
            String key = "id_" + Integer.toString(i);

            logger.info("Key: {}", key);

            ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(topic, key, value);
    
            // Send data
            producer.send(producerRecord,
                (recordMetadata, e) -> {
                    // Execute every time a record is successfully sent or an exception is thrown
                    if (Objects.isNull(e)) {
                        logger.info("Received new metadata\nTopic: {}\nPartition: {}\nOffset: {}\nTimestamp: {}",
                                recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset(),
                                recordMetadata.timestamp());
                    } else {
                        logger.error("Error while producing: {}", e);
                    }
                }).get(); // get() block the send to make it synchronous - don't do this in production  
                
        }
        // Flush data
        producer.flush();
        // Flush and close producer
        producer.close();
    }
}
