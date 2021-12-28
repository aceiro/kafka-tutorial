package kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemoAssignSeek {
    public static void main(String[] args) {
        Logger logger  = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class.getName());

        // create simple properties
        // String groupId          = "my-app";
        String bootstrapServer  = "127.0.0.1:9092";
        String earliest         = "earliest";
        String topic = "first_topic";
        long offset     = 15L;
        int partition   = 1;

        Properties p = new Properties();
        p.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        p.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, earliest);
        // p.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(p);

        // assign
        TopicPartition topicPartition = new TopicPartition(topic, partition);
        consumer.assign(Collections.singletonList(topicPartition));


        // seek
        consumer.seek(topicPartition, offset);

        int numberOfMessagesToRead = 5;
        boolean keepOnReading = true;
        int numberOfMessagesReadSoFar = 0;


        // poll for new data
        while(keepOnReading){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record: records) {
                numberOfMessagesReadSoFar++;
                logger.info("Key: {}", record.key());
                logger.info("Value: {}", record.value());
                logger.info("Partition: {} Offset: {}", record.partition(), record.offset());
                if(numberOfMessagesReadSoFar >= numberOfMessagesToRead) {
                    keepOnReading = false;
                    break;
                }
            }
        }
        logger.info("Exiting the application");
    }
}
