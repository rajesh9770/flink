package com.flink.examples;

import com.flink.utils.StreamUtil;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FilterCharacters {

    public static void main(String[] args) throws Exception {

        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        // get input data
        DataStream<String> text = StreamUtil.getDataStream(env, params);
        if(text==null){
            System.exit(1);
        }

        DataStream<String> outStream = text.filter(new StreamUtil.FilterSmallStrings());
        outStream.print();
        env.execute("FilterCharacters");
    }

}
