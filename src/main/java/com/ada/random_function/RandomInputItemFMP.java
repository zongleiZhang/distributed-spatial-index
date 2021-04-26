package com.ada.random_function;

import com.ada.common.Constants;
import com.ada.geometry.Segment;
import com.ada.model.common.input.InputItem;
import com.ada.model.common.input.InputItemKey;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

public class RandomInputItemFMP implements FlatMapFunction<InputItem, InputItemKey> {
    int i = 0;

    @Override
    public void flatMap(InputItem value, Collector<InputItemKey> out) {
        if (value instanceof Segment) {
            i++;
            i %= Constants.dividePartition;
            out.collect(new InputItemKey(Constants.divideSubTaskKeyMap.get(i), value));
        }else {
            for (int j = 0; j < Constants.dividePartition; j++) {
                out.collect(new InputItemKey(Constants.divideSubTaskKeyMap.get(j), value));
            }
        }
    }
}
