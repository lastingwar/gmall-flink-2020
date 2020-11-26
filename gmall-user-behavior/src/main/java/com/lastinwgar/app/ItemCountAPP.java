package com.lastinwgar.app;

import com.lastinwgar.bean.ItemCount;
import com.lastinwgar.bean.UserData;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;

/**
 * @author yhm
 * @create 2020-11-25 11:57
 */
public class ItemCountAPP {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 读取数据
        DataStreamSource<String> dataStreamSource = env.readTextFile("input/UserBehavior.csv");
        // 转换结构
        SingleOutputStreamOperator<UserData> userDataDS = dataStreamSource.map(new MapFunction<String, UserData>() {
            @Override
            public UserData map(String value) throws Exception {
                String[] split = value.split(",");

                return new UserData(
                        Long.parseLong(split[0]),
                        Long.parseLong(split[1]),
                        Integer.parseInt(split[2]),
                        split[3],
                        Long.parseLong(split[4]));
            }
        })
                // 设置EventTime
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserData>() {
                    @Override
                    public long extractAscendingTimestamp(UserData element) {
                        return element.getTimestamp() * 1000L;
                    }
                });

        // 过滤分组
        KeyedStream<UserData, Long> itemIdDS = userDataDS
                .filter(userData -> "pv".equals(userData.getBehavior()))
                .keyBy(new KeySelector<UserData, Long>() {
                    @Override
                    public Long getKey(UserData value) throws Exception {
                        return value.getItemId();
                    }
                });

        // 开窗聚合
        SingleOutputStreamOperator<ItemCount> itemCountDS = itemIdDS.timeWindow(Time.hours(1), Time.minutes(5))
                .aggregate(new ItemCountAggregateFunc(), new ItemCountWindowFunc());

        // 分组求top5
        SingleOutputStreamOperator<String> outputStreamOperator = itemCountDS
                .keyBy(itemCount -> itemCount.getWindowEnd())
                .process(new ItemCountKeyedProcessFunc(5));

        outputStreamOperator.print();

        env.execute();
    }

    private static class ItemCountAggregateFunc implements AggregateFunction<UserData, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(UserData value, Long accumulator) {

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


    private static class ItemCountWindowFunc implements WindowFunction<Long, ItemCount, Long, TimeWindow> {
        @Override
        public void apply(Long aLong, TimeWindow window, Iterable<Long> input, Collector<ItemCount> out) throws Exception {
            Iterator<Long> iterator = input.iterator();
            Long itemCount = iterator.next();
            long endTime = window.getEnd();
            out.collect(new ItemCount(aLong, endTime, itemCount));
        }
    }


    private static class ItemCountKeyedProcessFunc extends KeyedProcessFunction<Long, ItemCount, String> {

        private Integer topSize = 1;

        public ItemCountKeyedProcessFunc() {
        }

        public ItemCountKeyedProcessFunc(Integer topSize) {
            this.topSize = topSize;
        }

        // 声明状态
        private MapState<Long, ItemCount> itemMap;

        // 初始化状态
        @Override
        public void open(Configuration parameters) throws Exception {
            itemMap = getRuntimeContext().getMapState(new MapStateDescriptor<Long, ItemCount>("itemMap", Long.class, ItemCount.class));
        }

        @Override
        public void processElement(ItemCount value, Context ctx, Collector<String> out) throws Exception {
            // 添加数据到状态
            itemMap.put(value.getItemId(), value);
            // 启动定时器
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 1L);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            Iterator<ItemCount> iterator = itemMap.values().iterator();
            ArrayList<ItemCount> itemCounts = Lists.newArrayList(iterator);
            itemCounts.sort(new Comparator<ItemCount>() {
                @Override
                public int compare(ItemCount o1, ItemCount o2) {
                    return o2.getCount().compareTo(o1.getCount());
                }
            });

            StringBuilder result = new StringBuilder();

            Date date = new Date(itemCounts.get(0).getWindowEnd());
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

            result.append("===============\n").append(sdf.format(date)).append("\n");

            for (int i = 0; i < Math.min(topSize, itemCounts.size()); i++) {
                result.append("No ").append(i+1).append(":")
                        .append("  商品ID:")
                        .append(itemCounts.get(i).getItemId())
                        .append("  浏览量:")
                        .append(itemCounts.get(i).getCount())
                        .append("\n");
            }
            result.append("===============\n\n");

            // 每次算出都能清空状态
            itemMap.clear();
            Thread.sleep(1000);
            out.collect(result.toString());
        }
    }
}
