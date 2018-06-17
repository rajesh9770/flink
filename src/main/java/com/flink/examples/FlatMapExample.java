package com.flink.examples;

import com.flink.utils.StreamUtil;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

public class FlatMapExample {

    public static void main(String[] args) throws Exception {
        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        // get input data
        DataStream<String> text = StreamUtil.getDataStream(env, params);
        if (text == null) {
            System.exit(1);
        }

        DataStream<String> outStream = text.map((MapFunction<String, String>) s -> s.split(",")[1].trim());
        outStream = outStream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                Arrays.stream(s.split("\t")).forEach(collector::collect);
            }
        });

//        outStream = outStream.flatMap((FlatMapFunction<String, String>) (String s, Collector<String> collector) -> {
//                Arrays.stream(s.split("\t")).forEach(collector::collect); return; }
//                );

//        outStream = outStream.flatMap((FlatMapFunction<String, String>) (input, collector) -> {
//                    Arrays.stream(input.split("\t")).forEach(collector::collect);  });

        outStream.print();
        env.execute("FlatMapExample");

    }
}
