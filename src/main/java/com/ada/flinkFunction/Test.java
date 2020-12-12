package com.ada.flinkFunction;

import com.ada.geometry.TrackPoint;
import org.apache.flink.api.common.io.FileInputFormat;

import java.io.IOException;

public class Test extends FileInputFormat<TrackPoint>{
    @Override
    public boolean reachedEnd() throws IOException {
        return false;
    }

    @Override
    public TrackPoint nextRecord(TrackPoint reuse) throws IOException {
        return null;
    }
}
