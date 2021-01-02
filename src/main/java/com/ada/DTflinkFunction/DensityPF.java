package com.ada.DTflinkFunction;

import com.ada.common.Arrays;
import com.ada.common.Constants;
import com.ada.geometry.TrackPoint;
import com.ada.model.densityToGlobal.Density;
import com.ada.model.densityToGlobal.DensityToGlobalElem;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class DensityPF extends ProcessWindowFunction<TrackPoint, DensityToGlobalElem, Integer, TimeWindow> {
    private int[][] grids;

    @Override
    public void process(Integer key,
                        Context context,
                        Iterable<TrackPoint> elements,
                        Collector<DensityToGlobalElem> out) throws Exception {
        for (TrackPoint element : elements) {
            int row = (int) Math.floor(((element.data[0] - Constants.globalRegion.low.data[0])/(Constants.globalRegion.high.data[0] - Constants.globalRegion.low.data[0]))*(Constants.gridDensity+1.0));
            int col = (int) Math.floor(((element.data[1] - Constants.globalRegion.low.data[1])/(Constants.globalRegion.high.data[1] - Constants.globalRegion.low.data[1]))*(Constants.gridDensity+1.0));
            grids[row][col]++;
            out.collect(element);
        }
        if (context.window().getStart()%(Constants.balanceFre* Constants.windowSize) == 0){
            for (int i = 0; i < Constants.globalPartition; i++) {
                out.collect(new Density(Arrays.cloneIntMatrix(grids), i));
            }
            grids = new int[Constants.gridDensity+1][Constants.gridDensity+1];
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        grids = new int[Constants.gridDensity+1][Constants.gridDensity+1];
    }
}
