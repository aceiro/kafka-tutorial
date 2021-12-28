package kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);
        String bootstrapServer = "127.0.0.1:9092";

        // create producer properties
        Properties p = new Properties();
        p.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        p.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(p);

        for(int i=0; i<10; i++) {
            // create a producer record
            String topic = "first_topic";
            String key   = "id_" + i;
            String value = "hello, world" + i;


            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

            // send data - asynchronous
            producer.send(record, (recordMetadata, e) -> {
                // executes every time a record is successfully or an exception is thrown
                if (e == null) {
                    logger.info("Receive new metadata\n" +
                            " Key: {}\n" +
                            " Topic: {} \n" +
                            " Partition: {}\n" +
                            " Offset: {}\n" +
                            " Timestamp: {}", key, recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset(), recordMetadata.timestamp());
                } else {
                    logger.error("Error received", e);
                }
            }).get(); /*block the .send() to make it synchronous - don't do this in production*/
        }
        producer.flush();
        producer.close();
    }
}
