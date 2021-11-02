package com.chi;

import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

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

        socketDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<String>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                        .withTimestampAssigner(new SerializableTimestampAssigner<String>() {
                            @Override
                            public long extractTimestamp(String element, long recordTimestamp) {
                                String[] fields = element.split(" ");
                                Long aLong = new Long(fields[1]);
                                return aLong * 1000L;
                            }
                        }))
                .flatMap(new FlatMapFunction<String, Tuple3<String, String, Integer>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple3<String, String, Integer>> out) throws Exception {
                        String[] fields = value.split(" ");
                        out.collect(new Tuple3<>(fields[0], fields[1], 1));
                    }
                })
                .keyBy(data -> data.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .trigger(EventTimeTrigger.create()) //默认使用EventTimeTrigger
                .apply(new WindowFunction<Tuple3<String, String, Integer>, Tuple5<String, String, Integer, String, String>, String, TimeWindow>() {

                    @Override
                    public void apply(String s, TimeWindow window, Iterable<Tuple3<String, String, Integer>> input, Collector<Tuple5<String, String, Integer, String, String>> out) throws Exception {
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                        ArrayList<Tuple3<String, String, Integer>> inputs = new ArrayList<>();
                        input.forEach(in -> inputs.add(in));

                        out.collect(new Tuple5<String, String, Integer, String, String>(s, inputs.get(0).f1, inputs.size(), sdf.format(window.getStart()), sdf.format(window.getEnd())));
                        System.out.println(sdf.format(window.getStart()));
                    }
                })
                .print();

        env.execute("run watermark wc");
    }
}
