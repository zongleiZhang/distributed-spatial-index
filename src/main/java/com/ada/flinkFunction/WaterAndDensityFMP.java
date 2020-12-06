//package com.ada.flinkFunction;
//
//import com.ada.common.Constants;
//import com.ada.dispatchElem.OneTwoData;
//import com.ada.dispatchElem.OneTwoDensity;
//import com.ada.dispatchElem.OneTwoPoint;
//import com.ada.dispatchElem.OneTwoWater;
//import com.ada.geometry.TrackPoint;
//import org.apache.flink.api.common.functions.RichFlatMapFunction;
//import org.apache.flink.configuration.Configuration;
//import org.apache.flink.util.Collector;
//
//public class WaterAndDensityFMP extends RichFlatMapFunction<TrackPoint, OneTwoData> {
//    private final static long period = 1000;
//    private long preWater;
//    private long startWindow;
//    private int[][] density;
//
//    @Override
//    public void flatMap(TrackPoint point, Collector<OneTwoData> out) {
//        long minus = point.timestamp - preWater;
//        if (minus >= period) {
//            preWater = preWater + ((int) (minus/period))*period;
//            for (int i = 0; i < Constants.keyTIDPartition; i++)
//                out.collect(new OneTwoWater(i,preWater));
//            minus = preWater - startWindow;
//            if ( minus >= Constants.windowSize ){
//                startWindow = startWindow + ((int) (minus/Constants.windowSize))*Constants.windowSize;
//                for (int i = 0; i < Constants.keyTIDPartition; i++)
//                    out.collect(new OneTwoDensity(i,startWindow,density));
//                density = new int[Constants.gridDensity+1][Constants.gridDensity+1];
//            }
//        }
//        int row = (int) Math.floor(((point.data[0] - Constants.globalRegion.low.data[0])/(Constants.globalRegion.high.data[0] - Constants.globalRegion.low.data[0]))*(Constants.gridDensity+1.0));
//        int col = (int) Math.floor(((point.data[1] - Constants.globalRegion.low.data[1])/(Constants.globalRegion.high.data[1] - Constants.globalRegion.low.data[1]))*(Constants.gridDensity+1.0));
//        density[row][col] ++;
//        out.collect(new OneTwoPoint(point));
//    }
//
//    @Override
//    public void open(Configuration parameters)  {
//        preWater = 0;
//        density = new int[Constants.gridDensity+1][Constants.gridDensity+1];
//        startWindow = 0;
//    }
//}
