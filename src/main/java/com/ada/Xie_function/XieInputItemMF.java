package com.ada.Xie_function;

import com.ada.common.Constants;
import com.ada.model.Xie.XieInputItem;
import com.ada.model.common.input.InputItem;
import org.apache.flink.api.common.functions.MapFunction;

public class XieInputItemMF implements MapFunction<InputItem, XieInputItem> {
    private int num = 0;

    @Override
    public XieInputItem map(InputItem value) {
        XieInputItem item = new XieInputItem(Constants.globalSubTaskKeyMap.get(num), value);
        num = ++num%Constants.globalPartition;
        return item;
    }
}
