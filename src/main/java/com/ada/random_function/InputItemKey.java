package com.ada.random_function;

import com.ada.model.inputItem.InputItem;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

@Getter
@Setter
public class InputItemKey implements Serializable {
    public Integer key;
    public InputItem item;

    public InputItemKey() {
    }

    public InputItemKey(int key, InputItem item) {
        this.key = key;
        this.item = item;
    }
}
