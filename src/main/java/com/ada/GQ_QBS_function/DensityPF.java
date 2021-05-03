package com.ada.GQ_QBS_function;

import com.ada.common.Arrays;
import com.ada.common.Constants;
import com.ada.geometry.Point;
import com.ada.geometry.Segment;
import com.ada.model.GQ_QBS.densityToGlobal.Density;
import com.ada.model.GQ_QBS.densityToGlobal.DensityToGlobalElem;
import com.ada.model.common.input.InputItem;
import com.ada.model.common.input.QueryItem;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class DensityPF extends ProcessAllWindowFunction<InputItem, DensityToGlobalElem, TimeWindow> {
    private int[][] grids;

    @Override
    public void process(Context context,
                        Iterable<InputItem> elements,
                        Collector<DensityToGlobalElem> out){
        for (InputItem element : elements) {
            if (!(element instanceof QueryItem)){
                Point point = ((Segment) element).getRect().getCenter();
                int row = (int) Math.floor(((point.data[0] - Constants.globalRegion.low.data[0])/(Constants.globalRegion.high.data[0] - Constants.globalRegion.low.data[0]))*(Constants.gridDensity+1.0));
                int col = (int) Math.floor(((point.data[1] - Constants.globalRegion.low.data[1])/(Constants.globalRegion.high.data[1] - Constants.globalRegion.low.data[1]))*(Constants.gridDensity+1.0));
                grids[row][col]++;
            }
            out.collect(element);
        }
        if (context.window().getStart()%(Constants.logicWindow*Constants.windowSize) == 0){
          for (int i = 0; i < Constants.globalPartition; i++)
              out.collect(new Density(Arrays.cloneIntMatrix(grids), i));
          grids = new int[Constants.gridDensity+1][Constants.gridDensity+1];
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        grids = new int[Constants.gridDensity+1][Constants.gridDensity+1];
    }
}
