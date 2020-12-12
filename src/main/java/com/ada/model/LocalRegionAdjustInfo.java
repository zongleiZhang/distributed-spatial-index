package com.ada.model;

import com.ada.geometry.Rectangle;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
public class LocalRegionAdjustInfo implements GlobalToLocalValue {

    public List<Integer> migrateOutST;

    public List<Integer> migrateFromST;

    public Rectangle region;

    public LocalRegionAdjustInfo() {
    }

    public LocalRegionAdjustInfo(List<Integer> migrateOutST,
                                 List<Integer> migrateFromST,
                                 Rectangle region) {
        this.migrateOutST = migrateOutST;
        this.migrateFromST = migrateFromST;
        this.region = region;
    }

}
