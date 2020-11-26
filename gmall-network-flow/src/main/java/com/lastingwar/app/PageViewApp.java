package com.lastingwar.app;

import com.lastingwar.bean.PvCount;
import com.lastingwar.bean.UserData;
import com.lastingwar.utils.PageView.CountAggregateFunc;
import com.lastingwar.utils.PageView.CountWindowFunc;
import com.lastingwar.utils.PageView.SumKeyedProcessFunc;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.Random;

/**
 * @author yhm
 * @create 2020-11-25 20:57
 */
public class PageViewApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(8);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<String> dataStreamSource = env.readTextFile("input/UserBehavior.csv");

        SingleOutputStreamOperator<UserData> userDataDS = dataStreamSource
                .map(new MapFunction<String, UserData>() {
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
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserData>() {
                    @Override
                    public long extractAscendingTimestamp(UserData element) {
                        return element.getTimestamp() * 1000L;
                    }
                })
                ;

        // 过滤出pv,之后只需要对其进行count
        SingleOutputStreamOperator<PvCount> pvCountDS = userDataDS.filter(userData -> "pv".equals(userData.getBehavior()))
                .map(new MapFunction<UserData, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(UserData value) throws Exception {
                        Random random = new Random();
                        String key = "pv_" + random.nextInt(8);
                        return new Tuple2<String, Long>(key, 1L);
                    }
                })
                .keyBy(tuple -> tuple.f0)
                .timeWindow(Time.hours(1))
                .aggregate(new CountAggregateFunc(), new CountWindowFunc());



        // 重新按照windowEnd分组,统计输出
        SingleOutputStreamOperator<String> result = pvCountDS.keyBy(PvCount::getWindowEnd)
                .process(new SumKeyedProcessFunc());

        result.print();
        env.execute();
    }
}
