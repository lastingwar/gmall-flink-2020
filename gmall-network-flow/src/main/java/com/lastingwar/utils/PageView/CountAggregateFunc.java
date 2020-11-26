package com.lastingwar.utils.PageView;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * @author yhm
 * @create 2020-11-26 9:50
 */
public class CountAggregateFunc implements AggregateFunction<Tuple2<String, Long>, Long, Long> {


    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(Tuple2<String, Long> value, Long accumulator) {
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
