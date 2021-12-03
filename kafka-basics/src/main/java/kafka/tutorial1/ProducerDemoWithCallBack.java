package kafka.tutorial1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Properties;

public class ProducerDemoWithCallBack {
    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallBack.class);

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
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>("first_topic",
                    "hello world " + i);
    
            // Send data
            producer.send(producerRecord,
                (recordMetadata, e) -> {
                    // Execute every time a record is successfully sent or an exception is thrown
                    if (Objects.isNull(e)) {
                        logger.info("Received new metadata\nTopic: {}\nPartition: {}\nOffset: {}\nTimestamp: {}",
                                recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset(),
                                recordMetadata.timestamp());
                    } else {
                        logger.error("Error while producing: {}", e.getMessage());
                    }
                });
                
        }
        // Flush data
        producer.flush();
        // Flush and close producer
        producer.close();
    }
}
