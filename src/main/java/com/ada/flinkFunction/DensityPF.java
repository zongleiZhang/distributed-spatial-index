package com.ada.flinkFunction;

import com.ada.common.Constants;
import com.ada.geometry.Point;
import com.ada.geometry.Segment;
import com.ada.model.Density;
import com.ada.model.DensityToGlobalInt;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class DensityPF extends ProcessWindowFunction<Segment, DensityToGlobalInt, Integer, TimeWindow> {

    @Override
    public void process(Integer integer,
                        Context context,
                        Iterable<Segment> elements,
                        Collector<DensityToGlobalInt> out){
        int[][] grids = new int[Constants.gridDensity+1][Constants.gridDensity+1];
        for (Segment segment : elements) {
            Point point = segment.getRect().getCenter();
            int row = (int) Math.floor(((point.data[0] - Constants.globalRegion.low.data[0])/(Constants.globalRegion.high.data[0] - Constants.globalRegion.low.data[0]))*(Constants.gridDensity+1.0));
            int col = (int) Math.floor(((point.data[1] - Constants.globalRegion.low.data[1])/(Constants.globalRegion.high.data[1] - Constants.globalRegion.low.data[1]))*(Constants.gridDensity+1.0));
            grids[row][col]++;
            out.collect(segment);
        }
        for (int i = 0; i < Constants.globalPartition; i++)
            out.collect(new Density(grids, i));
    }
}
