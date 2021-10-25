package com.lyl.flink.test5;

import com.lyl.flink.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author lyl
 * @description: TODO
 * @date 2021/10/24 12:36
 */
public class Flink04_Process_SideOutput {

    public static void main(String[] args) throws Exception {

        //1 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2 读取端口数据并转换为JavaBean
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = env.socketTextStream("119.29.84.234", 9999)
                .map(data -> {
                   String[] spilt = data.split(",");
                   return new WaterSensor(Integer.parseInt(spilt[0]), Long.parseLong(spilt[1]), Integer.parseInt(spilt[2]));
                });

        SingleOutputStreamOperator<WaterSensor> result = waterSensorDS.process(new SpiltProcessFunc());

        result.print("medium");

        DataStream<Tuple2<Integer, Integer>> high = result.getSideOutput(new OutputTag<Tuple2<Integer, Integer>>("high"){});

        high.print("high");

        DataStream<Tuple2<Integer, Integer>> slow = result.getSideOutput(new OutputTag<Tuple2<Integer, Integer>>("low"){});
        slow.print("low");

        //执行任务
        env.execute();

    }

    public static class SpiltProcessFunc extends ProcessFunction<WaterSensor, WaterSensor>{

        @Override
        public void processElement(WaterSensor value, ProcessFunction<WaterSensor, WaterSensor>.Context ctx, Collector<WaterSensor> out) throws Exception {
            //去除水位线
            Integer vc = value.getVc();

            if (vc >= 30 && vc <= 70){
                out.collect(value);
            } else if (vc < 30) {
                ctx.output(new OutputTag<Tuple2<Integer, Integer>>("low"){}, new Tuple2<>(value.getId(), vc));
            } else {
                ctx.output(new OutputTag<Tuple2<Integer, Integer>>("high"){}, new Tuple2<>(value.getId(), vc));
            }
        }

    }

}
