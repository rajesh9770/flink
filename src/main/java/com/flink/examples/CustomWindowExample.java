package com.flink.examples;

import com.flink.utils.StreamUtil;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;


public class CustomWindowExample {

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
        DataStream<CourseCount> outputStream = text.map(s-> {
            String[] tokens = s.split(" ");
            if(tokens.length ==2){
                return new CourseCount(tokens[0], tokens[1], 1);
            }
            return null;
        }).returns(new TypeHint<CourseCount>() { }.getTypeInfo());

        WindowedStream<CourseCount, Tuple, GlobalWindow> windowedStream = outputStream.keyBy("staticKey").countWindow(5);
        DataStream<Map<String, Integer>> outStream = windowedStream.apply(
                (Tuple tuple, GlobalWindow globalWindow, Iterable<CourseCount> iterable,
                                                                            Collector<Map<String, Integer>> collector) ->
                {
                    final Map<String, Integer> map = new HashMap<>();

                    (iterable).forEach(signUp -> {
                        if (signUp.country.equals("US")) {
                            if (!map.containsKey(signUp.course)) {
                                map.put(signUp.course, 1);
                            } else {
                                map.put(signUp.course, map.get(signUp.course) + 1);
                            }
                        }
                    });
                    collector.collect(map);
                });
        outStream.print();
        env.execute();
    }


    public static class CourseCount{
        public Integer staticKey = 1;
        public String course, country;
        public Integer count;

        public CourseCount(){
        }

        public CourseCount(String course, String country, int count){
            this.count = count;
            this.country = country;
            this.course = course;
        }

        public void setCourse(String course) {
            this.course = course;
        }

        public String getCourse() {
            return course;
        }

        public void setCountry(String country) {
            this.country = country;
        }

        public String getCountry() {
            return country;
        }

        public void setCount(Integer count) {
            this.count = count;
        }

        public Integer getCount() {
            return count;
        }
    }

    private static class CollectUS implements WindowFunction<CourseCount, Map<String, Integer>, Tuple, GlobalWindow> {

        @Override
        public void apply(Tuple tuple, GlobalWindow globalWindow, Iterable<CourseCount> iterable,
                          Collector<Map<String, Integer>> collector) throws Exception {
            final Map<String, Integer> map = new HashMap<>();

            iterable.forEach(signUp -> {
                if (signUp.country.equals("US")){
                    if (!map.containsKey(signUp.course)) {
                        map.put(signUp.course, 1);
                    }else{
                        map.put(signUp.course, map.get(signUp.course) + 1);
                    }
                }
            });
            collector.collect(map);
        }
    }
}
