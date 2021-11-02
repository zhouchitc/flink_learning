//import org.apache.flink.api.common.eventtime.WatermarkStrategy;
//import org.apache.flink.api.common.functions.MapFunction;
//import org.apache.flink.api.java.utils.ParameterTool;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.datastream.DataStreamSource;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
//import org.apache.flink.streaming.api.windowing.time.Time;
//import org.apache.flink.streaming.api.windowing.triggers.Trigger;
//import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
//import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
//
//import java.text.SimpleDateFormat;
//import java.time.Duration;
//import java.util.Date;
//import java.util.List;
//
//public class UseCase111 {
//
//    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
//
//        ParameterTool parameterTool = ParameterTool.fromArgs(args);
//        String host = parameterTool.getRequired("host");
//        int port = parameterTool.getInt("port");
//        DataStreamSource<String> dataStreamSource = env.socketTextStream(host, port);
//        DataStream<SensorReading> sensorDataStream = dataStreamSource.filter(x -> !x.trim().isEmpty()).map(new MapFunction<String, SensorReading>() {
//            @Override
//            public SensorReading map(String value) throws Exception {
//                List<String> fields = Splitter.on(",").trimResults().splitToList(value);
//                return new SensorReading(fields.get(0), Long.valueOf(fields.get(1)), Double.valueOf(fields.get(2)));
//            }
//        }).assignTimestampsAndWatermarks(WatermarkStrategy.<SensorReading>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner((element, recordTimestamp) -> element.getTimestamp() * 1000));
//
//        DataStream<SensorReading> tumblingTemperatureStream = sensorDataStream.keyBy(SensorReading::getName)
//                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
//                .trigger(new UdfTrigger())
//                .maxBy("temperature");
//
//        tumblingTemperatureStream.print("tumbling");
//
//        env.execute("use case 111");
//    }
//
//
//    public static class UdfTrigger extends Trigger<SensorReading, TimeWindow> {
//        private static int flag = 0;
//
//        @Override
//        public TriggerResult onElement(SensorReading element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
//            flag++;
//
//            if (window.maxTimestamp() <= ctx.getCurrentWatermark()) {
//                return TriggerResult.FIRE;
//            }
//            ctx.registerEventTimeTimer(window.maxTimestamp());
//            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//            System.out.printf("正在处理 element: %s, %s, 窗口时间: [%s, %s]", element, dateFormat.format(new Date(timestamp)), dateFormat.format(new Date(window.getStart())), dateFormat.format(new Date(window.getEnd())));
//            System.out.println("正在处理 element: " + element);
//            if (flag >= 4) {
//                flag = 0;
//                return TriggerResult.FIRE;
//            }
//            return TriggerResult.CONTINUE;
//        }
//
//        @Override
//        public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
//            return TriggerResult.CONTINUE;
//        }
//
//        @Override
//        public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
//            return TriggerResult.FIRE;
//        }
//
//        @Override
//        public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
//            ctx.deleteEventTimeTimer(window.maxTimestamp());
//        }
//    }
//}
