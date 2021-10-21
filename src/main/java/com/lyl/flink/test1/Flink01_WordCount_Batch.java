package com.lyl.flink.test1;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author lyl
 * @description: TODO
 * @date 2021/10/21 13:38
 */
public class Flink01_WordCount_Batch {

    public static void main(String[] args) throws Exception {

        //1获取环境变量
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //2 读取文件数据
        DataSource<String> input = env.readTextFile("input/word.txt");

        //3压平
        FlatMapOperator<String, String> wordDS = input.flatMap(new MyFlatMapFunc());

        //4讲单词转换为元组
        MapOperator<String, Tuple2<String, Integer>> wordToOneDS = wordDS.map((MapFunction<String, Tuple2<String, Integer>>)value -> {
            return new Tuple2<>(value, 1);
        }).returns(Types.TUPLE(Types.STRING, Types.INT));
        //5分组
        UnsortedGrouping<Tuple2<String, Integer>> groupBy = wordToOneDS.groupBy(0);

        //6聚合
        AggregateOperator<Tuple2<String, Integer>> result = groupBy.sum(1);

        //打印结果
        result.print();

    }

    public static class MyFlatMapFunc implements FlatMapFunction<String, String>{

        @Override
        public void flatMap(String value, Collector<String> collector) throws Exception {
            String[] words = value.split(" ");
            for (String word : words){
                collector.collect(word);
            }
        }
    }


}
