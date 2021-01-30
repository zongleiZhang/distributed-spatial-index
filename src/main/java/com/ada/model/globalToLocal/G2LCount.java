package com.ada.model.globalToLocal;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class G2LCount implements G2LValue{
    public int count;

    public G2LCount() {
    }

    public G2LCount(int count) {
        this.count = count;
    }
}
