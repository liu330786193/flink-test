package com.lyl.flink.test4;

import com.lyl.flink.bean.OrderEvent;
import com.lyl.flink.bean.TxEvent;
import com.lyl.flink.bean.UserBehavior;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import javax.jdo.annotations.Order;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author lyl
 * @description: TODO
 * @date 2021/10/21 16:13
 */
public class Flink04_Practice_OrderReceipt {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> ds = env.readTextFile("input/UserBehavior.csv");

        DataStreamSource<String> orderStreamDS = env.readTextFile("input/OrderLog.csv");
        DataStreamSource<String> receiptStreamDS = env.readTextFile("input/ReceiptLog.csv");

        SingleOutputStreamOperator<OrderEvent> orderEventDS = orderStreamDS.flatMap(new FlatMapFunction<String, OrderEvent>() {
            @Override
            public void flatMap(String value, Collector<OrderEvent> collector) throws Exception {
                if (!value.contains("pay")){
                    return;
                }
                String[] spilt = value.split(",");
                OrderEvent orderEvent = new OrderEvent(
                        Long.parseLong(spilt[0]),
                        spilt[1],
                        spilt[2],
                        Long.parseLong(spilt[3])
                        );
                collector.collect(orderEvent);
            }
        });

        SingleOutputStreamOperator<TxEvent> txDS = receiptStreamDS.map(new MapFunction<String, TxEvent>() {
            @Override
            public TxEvent map(String value) throws Exception {
                String[] spilt = value.split(",");
                return new TxEvent(spilt[0], spilt[1], Long.parseLong(spilt[2]));
            }
        });

        //4 按照TXID进行分组
        KeyedStream<OrderEvent, String> orderEventStringKeyedStream = orderEventDS.keyBy(OrderEvent::getTxId);
        KeyedStream<TxEvent, String> txEventStringKeyedStream = txDS.keyBy(TxEvent::getTxId);

        //5 连接两个流
        ConnectedStreams<OrderEvent, TxEvent> connectedStreams = orderEventStringKeyedStream.connect(txEventStringKeyedStream);

        //6 处理两条流的数据
        SingleOutputStreamOperator<Tuple2<OrderEvent, TxEvent>> result = connectedStreams.process(new MyCoKeyedProcessFunc());

        result.print();
        env.execute();

    }

    public static class MyCoKeyedProcessFunc extends KeyedCoProcessFunction<String, OrderEvent, TxEvent, Tuple2<OrderEvent, TxEvent>>{

        private HashMap<String, OrderEvent> orderEventHashMap = new HashMap<>();
        private HashMap<String, TxEvent> txEventHashMap = new HashMap<>();


        @Override
        public void processElement1(OrderEvent value, KeyedCoProcessFunction<String, OrderEvent, TxEvent, Tuple2<OrderEvent, TxEvent>>.Context ctx, Collector<Tuple2<OrderEvent, TxEvent>> out) throws Exception {
            if (txEventHashMap.containsKey(value.getTxId())){
                TxEvent txEvent = txEventHashMap.get(value.getTxId());
                out.collect(new Tuple2<>(value, txEvent));
            }else {
                orderEventHashMap.put(value.getTxId(), value);
            }
        }

        @Override
        public void processElement2(TxEvent value, KeyedCoProcessFunction<String, OrderEvent, TxEvent, Tuple2<OrderEvent, TxEvent>>.Context ctx, Collector<Tuple2<OrderEvent, TxEvent>> out) throws Exception {
            if (orderEventHashMap.containsKey(value.getTxId())){
                OrderEvent orderEvent = orderEventHashMap.get(value.getTxId());
                out.collect(new Tuple2<>(orderEvent, value));
            } else {
                txEventHashMap.put(value.getTxId(), value);
            }

        }
    }

}





















