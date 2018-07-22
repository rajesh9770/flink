package com.flink.examples;

import com.flink.utils.StreamUtil;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import javax.xml.crypto.Data;


public class StateExample1 {

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
        DataStream<Tuple2<Integer, String>> stream = text.map(s -> Tuple2.of(1, s.trim().toLowerCase())).
                returns(new TypeHint<Tuple2<Integer, String>>() {}.getTypeInfo());

        DataStream<String> output = stream.keyBy(0).flatMap(new CollectDistinctWords());

        output.print();
        env.execute("word count");

    }

    public static class CollectDistinctWords extends RichFlatMapFunction<Tuple2<Integer, String>, String>{

        private transient ListState<String> distinctWords;

        @Override
        public void flatMap(Tuple2<Integer, String> input, Collector<String> out) throws Exception {
            Iterable<String> currentWordList = distinctWords.get();
            boolean found = false;
            if(input.f1.equals("print")){
                out.collect(currentWordList.toString());
                distinctWords.clear();
            }else{
                for( String word: currentWordList){
                    if(input.f1.equals(word)){
                        found = true;
                        break;
                    }
                }
                if(!found){
                    distinctWords.add(input.f1);
                }
            }
        }

        @Override
        public void open(Configuration config){
            ListStateDescriptor<String> descriptor = new ListStateDescriptor<>("wordList", String.class);
            distinctWords = getRuntimeContext().getListState(descriptor);
        }
    }
}
