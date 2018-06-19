package com.flink.examples;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;

import java.util.Properties;

public class TwitterExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties twitterProps = new Properties();

        twitterProps.setProperty(TwitterSource.CONSUMER_KEY, System.getenv("TwitterSource_CONSUMER_KEY"));
        twitterProps.setProperty(TwitterSource.CONSUMER_SECRET, System.getenv("TwitterSource_CONSUMER_SECRET"));
        twitterProps.setProperty(TwitterSource.TOKEN, System.getenv("TwitterSource_TOKEN"));
        twitterProps.setProperty(TwitterSource.TOKEN_SECRET, System.getenv("TwitterSource_TOKEN_SECRET"));

        System.out.println("HEREERER " + System.getenv("TwitterSource_CONSUMER_KEY") + " " +
                System.getenv("TwitterSource_CONSUMER_SECRET") + " " +
                System.getenv("TwitterSource_TOKEN") + " " +
                System.getenv("TwitterSource_TOKEN_SECRET")
        );
        DataStreamSource<String> twitterStream = env.addSource(new TwitterSource(twitterProps));
        SingleOutputStreamOperator<Tuple2<String, Integer>> locationTuples = twitterStream.map(json -> {
            ObjectMapper jsonMapper = new ObjectMapper();
            JsonNode jsonNode = jsonMapper.readValue(json, JsonNode.class);
            //System.out.println(jsonNode.toString());
            String location = "unknown";
            if (jsonNode.has("user") && jsonNode.get("user").has("location")) {
                location = jsonNode.get("user").get("location").textValue();
            }
            return new Tuple2<>(location, 1);
        }).returns(new TypeHint<Tuple2<String, Integer>>() { }.getTypeInfo());

        DataStream<Tuple2<String, Integer>> tweets = locationTuples.keyBy(0).window(TumblingProcessingTimeWindows.of(Time.seconds(60))).sum(1);
        tweets.print();
        env.execute();

    }
}
