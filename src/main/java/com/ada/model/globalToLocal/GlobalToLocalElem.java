package com.ada.model.globalToLocal;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

@Getter
@Setter
public class GlobalToLocalElem implements Serializable {
    public int key;

    /**
     * 元素类型：
     *  1: 索引项，value是Segment对象
     *  2: 查询项，value是QueryItem对象
     *  3: 局部索引Region调整，value是AdjustLocalRegion,
     *          当value是AdjustLocalRegion的region成员是null时表示废弃subtask
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
