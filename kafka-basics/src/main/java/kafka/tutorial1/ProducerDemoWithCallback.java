package kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);
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
            ProducerRecord<String, String> record = new ProducerRecord<>("first_topic", "hello, world " + i);

            // send data - asynchronous
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    // executes every time a record is successfully or an exception is thrown
                    if (e == null) {
                        logger.info("Receive new metadata\n" +
                                " Topic: {} \n" +
                                " Partition: {}\n" +
                                " Offset: {}\n" +
                                " Timestamp: {}", recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset(), recordMetadata.timestamp());
                    } else {
                        logger.error("Error received", e);
                    }
                }
            });
        }
        producer.flush();
        producer.close();
    }
}
