package com.ada.model.inputItem;

import com.ada.model.densityToGlobal.DensityToGlobalElem;
import com.ada.model.globalToLocal.GlobalToLocalValue;

import java.io.Serializable;

public interface InputItem extends DensityToGlobalElem, GlobalToLocalValue, Serializable {

    long getTimeStamp();
    int getInputKey();
}
