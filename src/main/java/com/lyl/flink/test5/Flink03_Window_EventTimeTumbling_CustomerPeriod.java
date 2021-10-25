package com.lyl.flink.test5;

import com.lyl.flink.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.time.Duration;

/**
 * @author lyl
 * @description: TODO
 * @date 2021/10/24 12:36
 */
public class Flink03_Window_EventTimeTumbling_CustomerPeriod {

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

        //3 提取数据中心的时间戳字段
        WatermarkStrategy<WaterSensor> waterSensorWatermarkStrategy = new WatermarkStrategy<WaterSensor>() {
            @Override
            public WatermarkGenerator<WaterSensor> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                return new MyPeriod(2000L);
            }
        };
        SingleOutputStreamOperator<WaterSensor> waterSensorSingleOutputStreamOperator = waterSensorDS
                .assignTimestampsAndWatermarks(waterSensorWatermarkStrategy);

        //4 按照id分组
        KeyedStream<WaterSensor, Integer> waterSensorIntegerKeyedStream = waterSensorSingleOutputStreamOperator.keyBy(WaterSensor::getId);

        //5 开窗 允许迟到数据，侧输出流
        WindowedStream<WaterSensor, Integer, TimeWindow> window = waterSensorIntegerKeyedStream.window(EventTimeSessionWindows.withGap(Time.seconds(5)));


        //6 计算总和
        SingleOutputStreamOperator<WaterSensor> result = window.sum("vc");

        //7 打印
        result.print();

        //执行任务
        env.execute();

    }

    //自定义周期性的Watermark生成器
    public static class MyPeriod implements WatermarkGenerator<WaterSensor>{

        private Long maxTs;
        private Long maxDelay;

        public MyPeriod(Long maxDelay){
            this.maxDelay = maxDelay;
            this.maxTs = Long.MIN_VALUE + maxDelay + 1;
        }


        @Override
        public void onEvent(WaterSensor event, long eventTimestamp, WatermarkOutput output) {
            System.out.println("获取数据中心最大的时间戳");
            maxTs = Math.max(eventTimestamp, maxTs);
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            System.out.println("生成WaterMark" + (maxTs - maxDelay));
            output.emitWatermark(new Watermark(maxTs - maxDelay));
        }
    }

}
