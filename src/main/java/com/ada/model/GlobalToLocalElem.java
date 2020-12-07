package com.ada.model;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

@Getter
@Setter
public class GlobalToLocalElem implements Serializable {
    public int key;

    /**
     * 元素类型：
     *  1. 索引项，value是Segment对象
     *  2. 查询项，value是Rectangle对象
     */
    public int elementType;

    public GlobalToLocalValue value;

    public GlobalToLocalElem() {
    }

    public GlobalToLocalElem(int key, int elementType, GlobalToLocalValue value) {
        this.key = key;
        this.elementType = elementType;
        this.value = value;
    }
}
