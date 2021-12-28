package kafka.tutorial2;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {
    Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());
    public TwitterProducer(){}

    public static void main(String[] args) {
        new TwitterProducer().run();
    }

    public void run(){
        logger.info("run()");
        BlockingDeque<String> msgQueue = new LinkedBlockingDeque<>(100000);

        // create twitter client
        Client client = createTwitterClient(msgQueue);
        client.connect();

        // create a kafka producer
        KafkaProducer<String, String> producer = createKafkaProducer();

        // add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() ->{
            logger.info("stopping application");
            logger.info("shutting down client from twitter...");
            client.stop();

            logger.info("closing producer...");
            producer.close();
            logger.info("done!!");

        }));

        // loop to send tweets to kafka
        // on a different thread, or multiple different threads....
        while (!client.isDone()) {
            String msg = null;
            try {
                 msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }
            if(msg!=null){
                logger.info(msg);
                producer.send(new ProducerRecord<>("tweets_topic", null, msg), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                             if(e!=null){
                                 logger.error("Error", e);
                             }
                    }
                });
            }
            logger.info("End of application");
        }
    }

    private KafkaProducer<String, String> createKafkaProducer() {
        String bootstrapServer = "127.0.0.1:9092";

        // create producer properties
        Properties p = new Properties();
        p.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        p.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create safe Producer
        p.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        p.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        p.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        p.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");

        // high throughput producer (at the expense of a bit of latency and CPU usage)
        p.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG,"snappy");
        p.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        p.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024)); // 32 KB batch size

        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(p);

        return producer;
    }

    public Client createTwitterClient(BlockingDeque<String> msgQueue){
        String consumerKey = "YF0Xl5syuNgLGO6peJxz3US4Z";
        String consumerSecretKey = "UenOsoyuv9UHEm0eNenBxk9KZQnYzs7udbjHox2lgRfSUmxMJ7";
        String token = "74705638-8i9RhiScvXEQCmrVAT5SePepJ1twWDzcokqi8aJlu";
        String secret = "OaDR5BN93HCoeo5aXbE7eOXNhJ5CNTvrvjGvrYLM9CMNz";



        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        List<String> terms = Lists.newArrayList("bitcoin", "usa", "politics", "sport", "soccer");
        hosebirdEndpoint.trackTerms(terms);

        // These secret should be read from a config file
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecretKey, token, secret);

        ClientBuilder builder = new ClientBuilder()
                .name("twitter-producer-client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        // Attempts to establish a connection.
        return builder.build();


    }
}
