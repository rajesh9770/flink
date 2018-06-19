package com.flink.examples;

import com.flink.utils.StreamUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class ReduceExample {

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

        /**
         * Input:
         * key1 10
         * key2 20
         * key1 25
         * ...
         * Output:
         * key1 10 1
         * key2 20 1
         * key1 25 1
         *
         */
        DataStream<Tuple3<String, Double, Integer>> outStream = text.map(s-> {
                    String[] tokens = s.split(" ");
                    if (tokens.length == 2) {
                        return new Tuple3<>(tokens[0], Double.parseDouble(tokens[1]), 1);
                    }
                    return null;
                }
        ).returns(new TypeHint<Tuple3<String, Double, Integer>>() { }.getTypeInfo());

        ReduceFunction<Tuple3<String, Double, Integer>> tReduceFunction =
                (cumulative, input) -> new Tuple3<>(cumulative.f0, cumulative.f1 + input.f1, cumulative.f2 + 1);
        /**
         * Output:
         * key1 35 2
         * key2 20 1
         */
        outStream = outStream.keyBy(0).reduce(tReduceFunction);

        /**
         * Output:
         * key1 17.5
         * key2 20.0
         */

        TypeInformation<Tuple2<String, Double>> tClass = new TypeHint<Tuple2<String, Double>>() { }.getTypeInfo();
        DataStream<Tuple2<String, Double>> outStream2 = outStream.map((MapFunction<Tuple3<String, Double, Integer>, Tuple2<String, Double>>) tuple ->
                new Tuple2<>(tuple.f0, tuple.f1 / tuple.f2)).returns(tClass);

        outStream2.print();
        env.execute("ReduceExample");
    }
}
