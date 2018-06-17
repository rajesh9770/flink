package com.flink.examples;

import com.flink.utils.StreamUtil;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Aggregation {

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

        DataStream<Tuple2<String, Double>> outStream = text.map(s-> {
                    String[] tokens = s.split(" ");
                    if(tokens.length ==2){
                        return new Tuple2<>(tokens[0], Double.parseDouble(tokens[1]));
                    }
                    return null;
                }
        ).returns(new TypeHint<Tuple2<String, Double>>() { }.getTypeInfo());

        outStream = outStream.keyBy(0).sum(1);
        outStream.print();
        env.execute("Aggregation");
    }
}