package com.lastingwar.app;

import com.lastingwar.bean.ApacheLog;
import com.lastingwar.bean.UrlViewCount;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;

/**
 * @author yhm
 * @create 2020-11-25 18:44
 */
public class HotUrlApp {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //读取数据
        DataStreamSource<String> dataStreamSource = env.readTextFile("input/apache.log");

//        DataStreamSource<String> dataStreamSource = env.socketTextStream("hadoop102", 7777);


        // 转换为javaBean
        SingleOutputStreamOperator<ApacheLog> logDS = dataStreamSource.map(new MapFunction<String, ApacheLog>() {
            @Override
            public ApacheLog map(String value) throws Exception {
                SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss");
                String[] split = value.split(" ");
                return new ApacheLog(
                        split[0],
                        split[1],
                        sdf.parse(split[3]).getTime(),
                        split[5],
                        split[6]);
            }
        });


        // 过滤并设置watermark
        SingleOutputStreamOperator<ApacheLog> apacheLogDS = logDS.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<ApacheLog>(Time.seconds(1)) {
                    @Override
                    public long extractTimestamp(ApacheLog element) {
                        return element.getEventTime();
                    }
                }).filter(apacheLog -> "GET".equals(apacheLog.getMethod()));

        OutputTag<ApacheLog> logOutputTag = new OutputTag<ApacheLog>("sideOutPut"){};

        // 分组开窗
        WindowedStream<ApacheLog, String, TimeWindow> apacheLogStringTimeWindowStream = apacheLogDS
                .keyBy(apacheLog -> apacheLog.getUrl())
                .timeWindow(Time.minutes(10), Time.seconds(5))
                .allowedLateness(Time.seconds(60))
                .sideOutputLateData(logOutputTag);

        // count整理
        SingleOutputStreamOperator<UrlViewCount> urlViewCountDS = apacheLogStringTimeWindowStream
                .aggregate(new HotUrlAggregateFunc(), new HotUrlWindowFunc());

        // 启动定时器输出topN
        SingleOutputStreamOperator<String> outputDS = urlViewCountDS.keyBy(urlViewCount -> urlViewCount.getWindowEnd())
                .process(new HotUrlProcessFunc(5));


        urlViewCountDS.getSideOutput(logOutputTag).print("sideOutPut");
        outputDS.print("result");
        env.execute();

    }



    /**
     * AggregateFunction 每条信息进行count
     */
    private static class HotUrlAggregateFunc implements AggregateFunction<ApacheLog, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(ApacheLog value, Long accumulator) {
            return ++accumulator;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }

    /**
     * WindowFunction 整个window的数据输出UrlViewCount
     */
    private static class HotUrlWindowFunc implements WindowFunction<Long, UrlViewCount,String,TimeWindow> {
        @Override
        public void apply(String s, TimeWindow window, Iterable<Long> input, Collector<UrlViewCount> out) throws Exception {
            Iterator<Long> iterator = input.iterator();
            Long itemCount = iterator.next();
            long endTime = window.getEnd();

            out.collect(new UrlViewCount(s, endTime, itemCount));
        }
    }

    /**
     * key:窗口关闭时间
     * 启动定时器,输出topN
     */
    private static class HotUrlProcessFunc extends KeyedProcessFunction<Long,UrlViewCount,String> {

        private Integer topSize;

        public HotUrlProcessFunc() {
        }

        public HotUrlProcessFunc(Integer topSize) {
            this.topSize = topSize;
        }

        // 定义状态
        private MapState<String,UrlViewCount> urlCountMap;

        @Override
        public void open(Configuration parameters) throws Exception {
            urlCountMap = getRuntimeContext().getMapState(new MapStateDescriptor<String, UrlViewCount>("urlCountMap", String.class, UrlViewCount.class));
        }

        @Override
        public void processElement(UrlViewCount value, Context ctx, Collector<String> out) throws Exception {
            urlCountMap.put(value.getUrl(),value);
            // 触发计算
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 1L);
            // 触发清空状态
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 60000L);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            // 清空状态
            if (ctx.getCurrentKey() + 60000L == timestamp){
                urlCountMap.clear();
                return;
            }


            ArrayList<Map.Entry<String, UrlViewCount>> entries = Lists.newArrayList(urlCountMap.entries().iterator());
            // 排序
            entries.sort(new Comparator<Map.Entry<String, UrlViewCount>>() {
                @Override
                public int compare(Map.Entry<String, UrlViewCount> o1, Map.Entry<String, UrlViewCount> o2) {
                    return o2.getValue().getCount().compareTo(o1.getValue().getCount());
                }
            });
            StringBuilder result = new StringBuilder();

            Date date = new Date(entries.get(0).getValue().getWindowEnd());
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

            result.append("================\n").append(sdf.format(date)).append("\n");

            for (int i = 0 ; i < Math.min(topSize,entries.size());i++){
                result.append("No ").append(i+1).append(":")
                        .append("  UTL:")
                        .append(entries.get(i).getValue().getUrl())
                        .append("  浏览量:")
                        .append(entries.get(i).getValue().getCount())
                        .append("\n");
            }
            result.append("===============\n\n");

            Thread.sleep(1000);
            out.collect(result.toString());
        }
    }
}
