package com.flink.utils;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StreamUtil<T> {
    public static DataStreamSource<String> getDataStream(StreamExecutionEnvironment env, final ParameterTool params){
        DataStreamSource<String> stream = null;
        if(params.has("input")){
            stream = env.readTextFile(params.get("input"));
        }else if(params.has("host") && params.has("port")){
            stream = env.socketTextStream(params.get("host"), Integer.parseInt(params.get("port")));
        }else{
            System.out.println("Provide either host/port or input file");
        }

        return stream;
    }

    public static class FilterSmallStrings implements org.apache.flink.api.common.functions.FilterFunction<String> {

        public boolean filter(String s) throws Exception {

            for(String token: s.split("\\s+")){
                try {
                    Double.parseDouble(s);
                    continue;
                }catch (Exception e){ }
                if(token.length()> 3) return false;
            }
            return true;
        }
    }

    public static class Lowercase implements org.apache.flink.api.common.functions.MapFunction<String, String> {

        public String map(String s) throws Exception {
            return s.toLowerCase();
        }
    }
}
