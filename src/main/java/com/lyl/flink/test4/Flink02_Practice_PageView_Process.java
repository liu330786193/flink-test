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
import scala.xml.Atom;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author lyl
 * @description: TODO
 * @date 2021/10/21 16:13
 */
public class Flink02_Practice_PageView_Process {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> ds = env.readTextFile("input/UserBehavior.csv");
        SingleOutputStreamOperator<UserBehavior> userBehaviorOS = ds.flatMap(new FlatMapFunction<String, UserBehavior>() {
            @Override
            public void flatMap(String value, Collector<UserBehavior> collector) throws Exception {
                String[] split = value.split(",");
                UserBehavior userBehavior = new UserBehavior(
                        Long.parseLong(split[0]),
                        Long.parseLong(split[1]),
                        Integer.parseInt(split[2]),
                        split[3],
                        Long.parseLong(split[4])
                );
                if ("pv".equals(userBehavior.getBehavior())) {
                    collector.collect(userBehavior);
                }
            }
        });
        KeyedStream<UserBehavior, String> keyedStream = userBehaviorOS.keyBy(data -> "PV");

        SingleOutputStreamOperator<AtomicInteger> result = keyedStream.process(new KeyedProcessFunction<String, UserBehavior, AtomicInteger>() {
            AtomicInteger count = new AtomicInteger(0);

            @Override
            public void processElement(UserBehavior value, KeyedProcessFunction<String, UserBehavior, AtomicInteger>.Context ctx, Collector<AtomicInteger> out) throws Exception {
                count.getAndIncrement();
                out.collect(count);
            }
        });

        result.print();
        env.execute();

    }
}





















