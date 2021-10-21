package com.lyl.flink.test1;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author lyl
 * @description: TODO
 * @date 2021/10/21 13:38
 */
public class Flink03_WordCount_unBounded {

    public static void main(String[] args) throws Exception {

        //1获取环境变量
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        //2 读取文件数据
//        DataStreamSource<String> input = env.readTextFile("input");;
        DataStreamSource<String> socketTextStream = env.socketTextStream("127.0.0.1", 9999);


        //3压平
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOneDS = socketTextStream.flatMap(new LineToTupleFlatMapFunc());

        //4讲单词转换为元组
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = wordToOneDS.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        });
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyedStream.sum(1);

        //6聚合
        result.print();

        //打印结果
        env.execute();

    }

    public static class LineToTupleFlatMapFunc implements FlatMapFunction<String, Tuple2<String, Integer>>{

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> collector) throws Exception {
            String[] words = value.split(" ");
            for (String word : words){
                collector.collect(new Tuple2<>(word, 1));
            }
        }
    }


}
