package com.ada.model;

import com.ada.geometry.Rectangle;
import lombok.Getter;
import lombok.Setter;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.List;

@Getter
@Setter
public class LocalRegionAdjustInfo implements GlobalToLocalValue {

    public List<Tuple2<Integer,Rectangle>> migrateOutST;

    public List<Integer> migrateFromST;

    public Rectangle region;

    public LocalRegionAdjustInfo() {
    }

    public LocalRegionAdjustInfo(List<Tuple2<Integer,Rectangle>> migrateOutST,
                                 List<Integer> migrateFromST,
                                 Rectangle region) {
        this.migrateOutST = migrateOutST;
        this.migrateFromST = migrateFromST;
        this.region = region;
    }

}
