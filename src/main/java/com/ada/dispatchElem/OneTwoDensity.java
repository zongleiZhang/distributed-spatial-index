package com.ada.dispatchElem;

public class OneTwoDensity extends OneTwoData{
    public long timeStamp;
    public int[][] density;

    public OneTwoDensity(){}

    public OneTwoDensity(Integer key, long timeStamp,int[][] density) {
        super(key);
        this.timeStamp = timeStamp;
        this.density = density;
    }

}
