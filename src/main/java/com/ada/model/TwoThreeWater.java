package com.ada.model;

public class TwoThreeWater extends TwoThreeData{
    public long water;

    public TwoThreeWater() {}

    public TwoThreeWater(Integer key, long water) {
        super(key);
        this.water = water;
    }
}
