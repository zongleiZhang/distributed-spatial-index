package com.ada.model.globalToLocal;

import java.io.Serializable;

public class Global2LocalElem implements Serializable {
    public int key;
    /**
     * 0:  添加经过点         (Global2LocalPoints)
     * 1:  添加topK点         (Global2LocalPoints)
     * 2： 新增经过轨迹   (Global2LocalPoints)
     * 3： 新增topK轨迹   (Global2LocalPoints)
     * 4： 删除经过轨迹   (Global2LocalTID)
     * 5： 删除topK轨迹   (Global2LocalTID)
     * 6.  经过轨迹改为topK轨迹 (Global2LocalTID)
     * 7.  topK轨迹改为经过轨迹 (Global2LocalTID)
     * 8： (调整负责区域)经过轨迹改为topK轨迹   (Global2LocalTID)
     * 9： (调整负责区域)topK轨迹改为经过轨迹   (Global2LocalTID)
     * 10： (调整负责区域)删除经过轨迹   (Global2LocalTID)
     * 11： (调整负责区域)删除topK轨迹   (Global2LocalTID)
     * 12：(调整负责区域)新增经过轨迹   (Global2LocalPoints)
     * 13：(调整负责区域)新增topK轨迹   (Global2LocalPoints)
     * 14: 通知Local subTask其新的负责区域   （Rectangle）
     */
    public byte flag;

    public Global2LocalValue value;

    public Global2LocalElem(int key, byte flag, Global2LocalValue value) {
        this.key = key;
        this.flag = flag;
        this.value = value;
    }

    public int getG2LKey(){
        return key;
    }
}
