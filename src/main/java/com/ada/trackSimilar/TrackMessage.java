package com.ada.trackSimilar;

import javafx.scene.media.Track;

import java.io.Serializable;
import java.util.List;

public class TrackMessage implements Message, Serializable {
    public List<TrackPoint> elems;
    public long timeStamp;

    public TrackMessage() {
    }

    public TrackMessage(List<TrackPoint> elems, long timeStamp) {
        this.elems = elems;
        this.timeStamp = timeStamp;
    }

    @Override
    public long getTimeStamp(){
        return timeStamp;
    }
}
