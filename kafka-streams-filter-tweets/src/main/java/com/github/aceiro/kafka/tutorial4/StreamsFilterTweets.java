package com.github.aceiro.kafka.tutorial4;


import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class StreamsFilterTweets {

    /*

    To create a new topic to pub message filtered
        kafka-topics.sh --bootstrap-server 127.0.0.1:9092 --topic important_tweets --create --partitions 3 --replication-factor 1

    To listening a new topic
        kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9092 --topic important_tweets

    */
    public static void main(String[] args) {
        // create properties
        Properties p = new Properties();
        p.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        p.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "demo-kafka-streams");
        p.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        p.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

        // create topology
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        // input topic
        KStream<String, String> inputTopic = streamsBuilder.stream("tweets_topic");
        KStream<String, String> filterStreams = inputTopic.filter(
                (k, jsonTweet) -> extractUserFollowersFromTweet(jsonTweet) > 10000
                // filter for tweets which has a user of iver 1k followers
        );
        filterStreams.to("important_tweets");

        // build the topology
        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), p);

        // start our streams application
        kafkaStreams.start();
    }

    private static int extractUserFollowersFromTweet(String tweetJson) {
        try {
            return JsonParser.parseString(tweetJson)
                    .getAsJsonObject()
                    .get("user")
                    .getAsJsonObject()
                    .get("followers_count")
                    .getAsInt();
        } catch (JsonSyntaxException e) {
            return 0;
        }
    }
}
