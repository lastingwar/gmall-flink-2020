package com.lastingwar.utils.PageView;

import com.lastingwar.bean.PvCount;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Date;
import java.text.SimpleDateFormat;

/**
 * @author yhm
 * @create 2020-11-26 10:13
 */
public class SumKeyedProcessFunc extends KeyedProcessFunction<Long, PvCount,String> {

    private ValueState<Long> counts ;
    @Override
    public void open(Configuration parameters) throws Exception {
        counts = getRuntimeContext().getState(new ValueStateDescriptor<Long>("counts", Long.class,0L));
    }

    @Override
    public void processElement(PvCount value, Context ctx, Collector<String> out) throws Exception {
        counts.update(counts.value() + value.getCount());
        ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 1L);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        StringBuilder result = new StringBuilder();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = new Date(timestamp - 1L);
        result.append("==================\n").append(sdf.format(date)).append(":\n");
        result.append("PV次数:").append(counts.value());

        Thread.sleep(1000L);
        out.collect(result.toString());
        counts.clear();
    }
}
