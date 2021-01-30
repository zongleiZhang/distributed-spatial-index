package com.ada.model.globalToLocal;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

@Getter
@Setter
public class G2TID implements G2LValue, Serializable {
    public int TID;

    public G2TID(int TID){
        this.TID = TID;
    }
}
