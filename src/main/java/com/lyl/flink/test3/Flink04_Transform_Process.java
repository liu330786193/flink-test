package com.lyl.flink.test3;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author lyl
 * @description: TODO
 * @date 2021/10/21 14:21
 */
public class Flink04_Transform_Process {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> socketTextStream = env.socketTextStream("127.0.0.1", 9999);
        SingleOutputStreamOperator<String> wordDS = socketTextStream.process(new ProcessFlatMapFunc());

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOneDS = wordDS.process(new ProcessFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void processElement(String value, ProcessFunction<String, Tuple2<String, Integer>>.Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                out.collect(new Tuple2<>(value, 1));
            }
        });
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = wordToOneDS.keyBy(data -> data.f0);
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyedStream.sum(1);
        result.print();
        env.execute();

    }

    public static class ProcessFlatMapFunc extends ProcessFunction<String, String>{

        @Override
        public void open(Configuration parameters) throws Exception {

        }

        @Override
        public void processElement(String value, ProcessFunction<String, String>.Context ctx, Collector<String> out) throws Exception {
            //运行时上下文，状态编程
            RuntimeContext runtimeContext = getRuntimeContext();

            String[] words = value.split(" ");
            for (String word : words){
                out.collect(word);
            }
            //定时器
            TimerService timerService = ctx.timerService();
//            timerService.registerEventTimeTimer(500L);

            //获取当前数据处理的时间
            timerService.currentProcessingTime();
            timerService.currentWatermark();


        }
    }

}























