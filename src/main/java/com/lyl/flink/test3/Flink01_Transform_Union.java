package com.lyl.flink.test3;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author lyl
 * @description: TODO
 * @date 2021/10/21 14:54
 */
public class Flink01_Transform_Union {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> socketTextStream1 = env.socketTextStream("127.0.0.1", 8888);
        DataStreamSource<String> socketTextStream2 = env.socketTextStream("127.0.0.1", 9999);

        DataStream<String> union = socketTextStream1.union(socketTextStream2);
        union.print();
        env.execute();

    }


}
