package com.lastingwar.tableapi;

import com.lastingwar.bean.UserData;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;

import java.util.Random;

/**
 * @author yhm
 * @create 2020-11-26 19:32
 */
public class TableApiPageView {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

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
                });

        Table fromDataStream = tableEnv.fromDataStream(userDataDS, "behavior,rt.rowtime");

        tableEnv.registerFunction("MyChange", new MyChange());
        Table select = fromDataStream.where("behavior = 'pv'")
                .select("behavior.MyChange as randomK,rt")
                .window(Tumble.over("1.hours").on("rt").as("tw"))
                .groupBy("randomK,tw")
                .select("randomK.count as firstCount,tw.end as endtime");

        Table result = select.groupBy("endtime")
                .select("firstCount.sum as counts , endtime");

        tableEnv.toRetractStream(result, Row.class).print();

        env.execute();


    }
    public static class MyChange extends ScalarFunction {

        public String eval(String value) {
            Random random = new Random();
            return (value + random.nextInt(8));
        }

    }
}
