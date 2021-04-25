package com.ada.Xie_function;

import com.ada.Xie_function.STRTree.STRTree;
import com.ada.common.Constants;
import com.ada.geometry.Rectangle;
import com.ada.geometry.Segment;
import com.ada.model.Xie.XieInputItem;
import com.ada.model.common.input.QueryItem;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.List;

public class XieGlobalPF extends ProcessWindowFunction<XieInputItem, XieInputItem, Integer, TimeWindow> {
    private STRTree tree;

    public XieGlobalPF(STRTree tree){
        this.tree = tree;
    }

    @Override
    public void process(Integer key,
                        Context context,
                        Iterable<XieInputItem> elements,
                        Collector<XieInputItem> out) {
        for (XieInputItem element : elements) {
            Rectangle rect;
            if (element.item instanceof Segment){
                rect = ((Segment) element.item).rect;
            }else {
                rect = ((QueryItem) element.item).rect;
            }
            List<Integer> leaves = tree.searchLeafIDs(rect);
            for (Integer leaf : leaves){
                out.collect(new XieInputItem(Constants.divideSubTaskKeyMap.get(leaf), element.item));
            }
        }
    }
}





























