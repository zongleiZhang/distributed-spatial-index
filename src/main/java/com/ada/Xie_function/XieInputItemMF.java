package com.ada.Xie_function;

import com.ada.common.Constants;
import com.ada.model.common.input.InputItem;
import com.ada.model.common.input.InputItemKey;
import org.apache.flink.api.common.functions.MapFunction;

public class XieInputItemMF implements MapFunction<InputItem, InputItemKey> {
    private int num = 0;

    @Override
    public InputItemKey map(InputItem value) {
        InputItemKey item = new InputItemKey(Constants.globalSubTaskKeyMap.get(num), value);
        num = ++num%Constants.globalPartition;
        return item;
    }
}
