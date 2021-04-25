package com.ada.model.Xie;

import com.ada.model.common.input.InputItem;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

@Getter
@Setter
public class XieInputItem implements Serializable {
    public int key;
    public InputItem item;

    public XieInputItem(int key, InputItem item) {
        this.key = key;
        this.item = item;
    }
}
