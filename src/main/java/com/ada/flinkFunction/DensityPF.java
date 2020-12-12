package com.ada.flinkFunction;

import com.ada.common.Constants;
import com.ada.geometry.Point;
import com.ada.geometry.Segment;
import com.ada.model.Density;
import com.ada.model.DensityToGlobalElem;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class DensityPF extends ProcessWindowFunction<Segment, DensityToGlobalElem, Integer, TimeWindow> {

    private int count = 1;
    private int[][] grids = new int[Constants.gridDensity+1][Constants.gridDensity+1];

    @Override
    public void process(Integer integer,
                        Context context,
                        Iterable<Segment> elements,
                        Collector<DensityToGlobalElem> out){

        for (Segment segment : elements) {
            Point point = segment.getRect().getCenter();
            int row = (int) Math.floor(((point.data[0] - Constants.globalRegion.low.data[0])/(Constants.globalRegion.high.data[0] - Constants.globalRegion.low.data[0]))*(Constants.gridDensity+1.0));
            int col = (int) Math.floor(((point.data[1] - Constants.globalRegion.low.data[1])/(Constants.globalRegion.high.data[1] - Constants.globalRegion.low.data[1]))*(Constants.gridDensity+1.0));
            grids[row][col]++;
            out.collect(segment);
        }
        if (count%Constants.balanceFre == 0){
            for (int i = 0; i < Constants.globalPartition; i++)
                out.collect(new Density(grids, i));
            grids = new int[Constants.gridDensity+1][Constants.gridDensity+1];
        }
        count++;
    }
}
