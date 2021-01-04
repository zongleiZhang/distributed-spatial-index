package com.ada.model.globalToLocal;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

@Getter
@Setter
public class Global2LocalTID implements Global2LocalValue, Serializable {
    public int TID;

    public Global2LocalTID(int TID){
        this.TID = TID;
    }
}
