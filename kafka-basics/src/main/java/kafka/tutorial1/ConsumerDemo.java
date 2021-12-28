package kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemo {
    public static void main(String[] args) {
        Logger logger  = LoggerFactory.getLogger(ConsumerDemo.class.getName());

        // create simple properties
        String groupId          = "my-app";
        String bootstrapServer  = "127.0.0.1:9092";
        String earliest         = "earliest";

        Properties p = new Properties();
        p.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        p.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        p.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, earliest);

        // create consumer
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(p);

        // subscribe
        kafkaConsumer.subscribe(Collections.singleton("first_topic"));

        // poll for new data
        while(true){
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record: records) {
                logger.info("Key: {}", record.key());
                logger.info("Value: {}", record.value());
                logger.info("Partition: {} Offset: {}", record.partition(), record.offset());
            }
        }

    }
}
