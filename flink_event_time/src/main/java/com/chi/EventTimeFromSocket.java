package com.chi;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author chi
 * @Description: TODO
 * @date 2021/10/27 15:42
 * @Version 1.0
 */
public class EventTimeFromSocket {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //flink1.12之后,此方法已经废弃,此处作为标注留在这里
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStreamSource<String> socketDS = env.socketTextStream("localhost", 9000);

        env.getConfig().setAutoWatermarkInterval(200L);


        SerializableTimestampAssigner<String> timestampAssigner = new SerializableTimestampAssigner<String>() {
            @Override
            public long extractTimestamp(String element, long recordTimestamp) {
                String[] fields = element.split(",");
                Long aLong = new Long(fields[0]);
                return aLong * 1000L;
            }
        };
        socketDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<String>forBoundedOutOfOrderness(
                        Duration.ofSeconds(10)).withTimestampAssigner(
                        new SerializableTimestampAssigner<String>() {
                            @Override
                            public long extractTimestamp(String element, long recordTimestamp) {
                                String[] fields = element.split(",");
                                Long aLong = new Long(fields[0]);
                                return aLong * 1000L;
                            }
                        })).flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] fields = value.split(",");
                out.collect(new Tuple2<>(fields[1], 1));
            }
        }).keyBy(data -> data.f0).window(TumblingEventTimeWindows.of(Time.seconds(10))).sum(1).print();

        env.execute("run watermark wc");

    }
}
