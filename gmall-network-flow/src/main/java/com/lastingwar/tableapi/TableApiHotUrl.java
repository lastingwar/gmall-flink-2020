package com.lastingwar.tableapi;

import com.lastingwar.bean.ApacheLog;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Slide;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;

/**
 * @author yhm
 * @create 2020-11-26 11:03
 */
public class TableApiHotUrl {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 读取数据并转换为JavaBean
        DataStreamSource<String> dataStreamSource = env.readTextFile("input/apache.log");

//        DataStreamSource<String> dataStreamSource = env.socketTextStream("hadoop102", 7777);
        SingleOutputStreamOperator<ApacheLog> logDS = dataStreamSource
                .map(new MapFunction<String, ApacheLog>() {
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
                }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<ApacheLog>(Time.seconds(1)) {
                    @Override
                    public long extractTimestamp(ApacheLog element) {
                        return element.getEventTime();
                    }
                });

        Table apacheLog = tableEnv.fromDataStream(logDS, "method,url,rt.rowtime");

        Table logCount = apacheLog.where("'GET' = method")
                .window(Slide.over("10.minutes").every("5.seconds").on("rt").as("tw"))
                .groupBy("url,tw")
                .select("url,url.count as counts,tw.end as endtime");

//        Thread.sleep(1);
        tableEnv.registerFunction("Top5Count", new Top5Count());

        Table result = logCount.groupBy("endtime")
                .flatAggregate("Top5Count(url,counts,endtime) as (endtime1,rk,url1,counts1)")
                .select("endtime1,rk,url1,counts1");

        Table result1 = result.groupBy("endtime1,rk")
                .select("endtime1,rk,url1.max as url2,counts1.max as counts2");


        String sinkDDL = "create table jdbcOutputTable (" +
                " endtime1 bigint not null, " +
                " rk int not null, " +
                " url2 varchar(256) not null, " +
                " counts2 bigint not null" +
                ") with (" +
                " 'connector.type' = 'jdbc', " +
                " 'connector.url' = 'jdbc:mysql://hadoop102:3306/test', " +
                " 'connector.table' = 'hoturl', " +
                " 'connector.driver' = 'com.mysql.jdbc.Driver', " +
                " 'connector.username' = 'root', " +
                " 'connector.password' = '123456', " +
                " 'connector.write.flush.max-rows' = '100'," + //刷写条数,默认5000
                " 'connector.write.flush.interval' = '2s')"; // 刷写时间,默认0s不启用

//        tableEnv.toRetractStream(result1, Row.class).print("result1");

        tableEnv.sqlUpdate(sinkDDL);

        tableEnv.insertInto("jdbcOutputTable", result1);

        env.execute();
    }

    public static class Top5Count extends TableAggregateFunction<Tuple4<Long, Integer, String, Long>, ArrayList<Tuple2<String, Long>>> {

        private Long end;

        @Override
        public ArrayList<Tuple2<String, Long>> createAccumulator() {
            end = 0L;
            return new ArrayList<Tuple2<String, Long>>(16) {
            };
        }

        public void accumulate(ArrayList<Tuple2<String, Long>> buffer, String url, Long counts, Timestamp endtime) {
            if (end == 0L) {
                end = endtime.getTime();
            }
            // 更新元素
            boolean flag = false;
            for (int i = 0; i < buffer.size(); i++) {
                Tuple2<String, Long> stringLongTuple2 = buffer.get(i);
                if (stringLongTuple2.f0.equals(url)) {
                    buffer.set(i,new Tuple2<>(url, Math.max(counts,stringLongTuple2.f1)));
                    flag = true;
                    break;
                }
            }
            if(!flag){
                buffer.add(new Tuple2<>(url,counts));
            }
        }

        public void emitValue(ArrayList<Tuple2<String, Long>> buffer, Collector<Tuple4<Long, Integer, String, Long>> collector) {

            buffer.sort(new Comparator<Tuple2<String, Long>>() {
                @Override
                public int compare(Tuple2<String, Long> o1, Tuple2<String, Long> o2) {
                    return o2.f1.compareTo(o1.f1);
                }
            });

            for (int i = 0; i < Math.min(buffer.size(), 5); i++) {
                collector.collect(new Tuple4<>(end, i + 1, buffer.get(i).f0, buffer.get(i).f1));
            }
        }
    }
}
