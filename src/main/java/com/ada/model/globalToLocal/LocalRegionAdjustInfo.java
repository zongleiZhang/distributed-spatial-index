package com.ada.model.globalToLocal;

import com.ada.geometry.Rectangle;
import com.ada.model.globalToLocal.GlobalToLocalValue;
import lombok.Getter;
import lombok.Setter;
import org.apache.flink.api.java.tuple.Tuple2;

import java.io.Serializable;
import java.util.List;

@Getter
@Setter
public class LocalRegionAdjustInfo implements GlobalToLocalValue, Serializable {

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
