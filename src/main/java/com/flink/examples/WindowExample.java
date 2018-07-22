package com.flink.examples;

import com.flink.utils.StreamUtil;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;



public class WindowExample {

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
        DataStream<Tuple2<String, Double>> outputStream = text.map(s-> {
            String[] tokens = s.split(" ");
            if(tokens.length ==2){
                return new Tuple2<>(tokens[0], Double.parseDouble(tokens[1]));
            }
            return null;
        }).returns(new TypeHint<Tuple2<String, Double>>() { }.getTypeInfo());
        //For Session Window of gap 10 sec   (non-fix size of events - all events that happen during that session/non-overlapping)
        //new session window will be started once a session is ideal for more than 10 sec.
        DataStream<Tuple2<String, Double>> sum = outputStream.keyBy(0).window(ProcessingTimeSessionWindows.withGap(Time.seconds(10))).sum(1);
        //DataStream<Tuple2<String, Double>> sum = outputStream.windowAll(ProcessingTimeSessionWindows.withGap(Time.seconds(10))).sum(1);
        sum.print();
        env.execute("TumblingWindow");
    }

    public static void mainForCountWindow(String[] args) throws Exception {
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
        DataStream<Tuple2<String, Double>> outputStream = text.map(s-> {
            String[] tokens = s.split(" ");
            if(tokens.length ==2){
                return new Tuple2<>(tokens[0], Double.parseDouble(tokens[1]));
            }
            return null;
        }).returns(new TypeHint<Tuple2<String, Double>>() { }.getTypeInfo());
        //For Count Window of size 2 events  (fix size of events/non-overlapping) only applicable to keyed stream.
        // a separate window will be created for each key. Each window will be processed when 2 keys are received.
        DataStream<Tuple2<String, Double>> sum = outputStream.keyBy(0).countWindow(2).sum(1);
        sum.print();
        env.execute("TumblingWindow");
    }

    public static void mainForKeyedStream(String[] args) throws Exception {
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
        DataStream<Tuple2<String, Double>> outputStream = text.map(s-> {
            String[] tokens = s.split(" ");
            if(tokens.length ==2){
                return new Tuple2<>(tokens[0], Double.parseDouble(tokens[1]));
            }
            return null;
        }).returns(new TypeHint<Tuple2<String, Double>>() { }.getTypeInfo());
        //For Tumbling Window of size 15 sec (fix size/non-overlapping)
        //DataStream<Tuple2<String, Double>> sum = outputStream.keyBy(0).window(TumblingProcessingTimeWindows.of(Time.seconds(15))).sum(1);
        //For Sliding Window of size 15 sec and sliding interval 10 sec  (fix size/overlapping)
        DataStream<Tuple2<String, Double>> sum = outputStream.keyBy(0).window(SlidingProcessingTimeWindows.of(Time.seconds(15), Time.seconds(10))).sum(1);
        sum.print();
        env.execute("TumblingWindow");
    }

    public static void mainForNonKeyedStream(String[] args) throws Exception {
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
        DataStream<Tuple2<String, Double>> outputStream = text.map(s-> {
            String[] tokens = s.split(" ");
            if(tokens.length ==2){
                return new Tuple2<>(tokens[0], Double.parseDouble(tokens[1]));
            }
            return null;
        }).returns(new TypeHint<Tuple2<String, Double>>() { }.getTypeInfo());
        //DataStream<Tuple2<String, Double>> sum = outputStream.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(15))).sum(1);
        DataStream<Tuple2<String, Double>> sum = outputStream.windowAll(SlidingProcessingTimeWindows.of(Time.seconds(15), Time.seconds(10))).sum(1);
        sum.print();
        env.execute("TumblingWindow");
    }
}
