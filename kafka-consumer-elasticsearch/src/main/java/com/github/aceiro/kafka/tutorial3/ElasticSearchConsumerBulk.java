package com.github.aceiro.kafka.tutorial3;


import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import static java.lang.Thread.sleep;

public class ElasticSearchConsumerBulk {

    /*
        ElasticSearch

        To fix the issue by increasing the value of index.mapping.total_fields.limit (default 1000)
        PUT /twitter/_settings
        {
            "index.mapping.total_fields.limit": 2000
        }

        To create a new index
        PUT /twitter/
        {}

        To get a Twitter message (tweet) that was produced by ElasticSearchConsumer
        GET /twitter/tweets/<ID>
        example
        GET twitter/tweets/kxf-7H0BZpQayMPbWZpR

        Kafka Groups
        Replaying data
        kafka-consumer-groups.sh --bootstrap-server 127.0.0.1:9092 --group kafka-demo-elasticsearch --describe
        kafka-consumer-groups.sh --bootstrap-server 127.0.0.1:9092 --group kafka-demo-elasticsearch --execute  --reset-offsets --to-earliest --topic tweets_topic --to-earliest
    *
    * */

    public static RestHighLevelClient createClient() {

        String password = "iqBDozrW2ecvQbS";
        String username = "YMbdrsFWZn";
        String hostname = "kafka-testing-178017710.us-east-1.bonsaisearch.net";


        BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));

        RestClientBuilder builder = RestClient.builder(new HttpHost(hostname, 443, "https"));
        builder.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
            @Override
            public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
            }
        });
        RestHighLevelClient client = new RestHighLevelClient(builder);
        return client;
    }

    public static KafkaConsumer<String, String> createConsumer(String topic){
        // create simple properties
        String bootstrapServer  = "127.0.0.1:9092";
        String earliest         = "earliest";
        String groupId          = "kafka-demo-elasticsearch";

        Properties p = new Properties();
        p.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        p.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        p.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, earliest);  /*this implicates to use idempotent mechanism*/
        p.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");  /*this disables at-least strategy and forces at-most. In this case,
                                                                            offsets will not commit automatically by the Kafka after 5 seconds
                                                                            as configured by default in auto.commit.interval.in.ms = 5000*/
        p.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");

        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(p);
        consumer.subscribe(Arrays.asList(topic));
        return consumer;
    }

    public static void main(String[] args) throws IOException {
        Logger logger = LoggerFactory.getLogger(ElasticSearchConsumerBulk.class.getName());
        // create Elasticsearch client
        RestHighLevelClient client = createClient();

        // Kafka consumer
        KafkaConsumer<String, String> consumer = createConsumer("tweets_topic");

        // poll for new data
        while(true){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            // Batch of records
            int recordCount = records.count();
            logger.info("Received {}", recordCount);
            BulkRequest bulkRequest = new BulkRequest();
            for (ConsumerRecord<String, String> record: records) {
                // To make idempotent we have 2 approaches
                // Kafka generic ID or source partner specific ID (in this case, Twitter)
                // 1.
                // String id = record.topic() + "_" + record.partition() + "_" + record.offset();

                // 2.
                try {
                    String id = extractIdFromTweet(record.value());

                    // where we insert data into ElasticSearch
                    String valueFromElasticSearch = record.value();
                    IndexRequest indexRequest = new IndexRequest(
                            "twitter",
                            "tweets",
                            id) /*id to make idempotent*/
                            .source(valueFromElasticSearch, XContentType.JSON);

                    bulkRequest.add(indexRequest);
                } catch (NullPointerException e) {
                    logger.warn("skipping bad data {}"+record.value());
                }
            }
            if(recordCount>0) {
                BulkResponse bulkItemResponses = client.bulk(bulkRequest, RequestOptions.DEFAULT);
                logger.info("Committing offsets...");
                consumer.commitSync();
                logger.info("Offsets have been committed...");
                delay(1000);
            }
        }
    }

    private static void delay(int delayInMilliseconds) {
        try {
            sleep(delayInMilliseconds);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


    private static String extractIdFromTweet(String value) {
        String id = JsonParser.parseString(value)
                        .getAsJsonObject()
                        .get("id_str")
                        .getAsString();
        return id;
    }
}
