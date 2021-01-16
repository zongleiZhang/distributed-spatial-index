package com.ada.DTflinkFunction;

import org.apache.flink.api.java.functions.KeySelector;

public class RebalancedKeySelector<T> implements KeySelector<T, Integer> {
    private int parallelism;
    private int count;

    public RebalancedKeySelector(int parallelism){
        this.parallelism = parallelism;
        this.count = 0;
    }

    @Override
    public Integer getKey(T value) {
        count = count%parallelism;
        count++;
        return count;
    }
}
