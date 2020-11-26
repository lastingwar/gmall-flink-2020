package com.lastingwar.utils.PageView;

import com.lastingwar.bean.PvCount;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author yhm
 * @create 2020-11-26 9:54
 */
public class CountWindowFunc implements WindowFunction<Long, PvCount,String, TimeWindow> {
    @Override
    public void apply(String s, TimeWindow window, Iterable<Long> input, Collector<PvCount> out) throws Exception {
        Long count = input.iterator().next();
        out.collect(new PvCount(s,window.getEnd(),count));
    }
}
