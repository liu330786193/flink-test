package com.lyl.flink.test4;

import com.lyl.flink.bean.UserBehavior;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author lyl
 * @description: TODO
 * @date 2021/10/21 16:13
 */
public class Flink01_Practice_PageView_WordCount {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> ds = env.readTextFile("input/UserBehavior.csv");
        SingleOutputStreamOperator<Tuple2<String, Integer>> userBehaviorOS = ds.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] split = value.split(",");
                UserBehavior userBehavior = new UserBehavior(
                        Long.parseLong(split[0]),
                        Long.parseLong(split[1]),
                        Integer.parseInt(split[2]),
                        split[3],
                        Long.parseLong(split[4])
                );
                if ("pv".equals(userBehavior.getBehavior())) {
                    collector.collect(new Tuple2<>("PV", 1));
                }
            }
        });
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = userBehaviorOS.keyBy(data -> data.f0);
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyedStream.sum(1);

        result.print();
        env.execute();

    }
}





















