package com.ada.flinkFunction;

import com.ada.common.Constants;
import com.ada.geometry.TrackPoint;
import org.apache.flink.api.java.functions.KeySelector;
import org.roaringbitmap.RoaringBitmap;

import java.util.ArrayList;
import java.util.List;

public class DensityKeySelector implements KeySelector<TrackPoint, Integer> {
    List<RoaringBitmap> bitmaps;
    int factor;
    int parallelism;

    public DensityKeySelector(int parallelism){
        bitmaps = new ArrayList<>(parallelism);
        for (int i = 0; i < parallelism; i++)
            bitmaps.add(new RoaringBitmap());
        factor = 0;
        this.parallelism = parallelism-1;
    }

    @Override
    public Integer getKey(TrackPoint value) {
        Integer key;
        int TID = value.TID;
        for (int i = 0; i < bitmaps.size(); i++) {
            if (bitmaps.get(i).contains(TID)) {
                key = Constants.densitySubTaskKeyMap.get(i);
                if (key == null)
                    System.out.print("");
                return key;
            }
        }
        factor = (factor + 1) & parallelism;
        bitmaps.get(factor).add(TID);
        key = Constants.densitySubTaskKeyMap.get(factor);
        if (key == null)
            System.out.print("");
        return key;
    }
}
