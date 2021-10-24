package com.lyl.flink.test4;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author lyl
 * @description: TODO
 * @date 2021/10/24 11:08
 */
public class Flink07_Window_CountTumbling {

    public static void main(String[] args) throws Exception {
        //1 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2 读取端口数据
        DataStreamSource<String> DSS = env.socketTextStream("127.0.0.1", 9999);

        //3 压平并转换为元组
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOneDS = DSS.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] words = value.split(" ");
                for (String word : words){
                    collector.collect(new Tuple2<>(word, 1));
                }
            }
        });

        //4 按照单词分组
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = wordToOneDS.keyBy(data -> data.f0);

        //5 开启技术的关东窗口
        WindowedStream<Tuple2<String, Integer>, String, GlobalWindow> windowedStream = keyedStream.countWindow(5L);

        //6 聚合计算
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = windowedStream.sum(1);

        //7 打印
        result.print();

        //8 执行任务
        env.execute();

    }

}
