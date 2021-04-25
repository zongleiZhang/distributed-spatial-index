package com.ada.model.common.input;

import com.ada.model.GQ_QBS.densityToGlobal.DensityToGlobalElem;
import com.ada.model.GQ_QBS.globalToLocal.GlobalToLocalValue;

import java.io.Serializable;

public interface InputItem extends DensityToGlobalElem, GlobalToLocalValue, Serializable {

    long getTimeStamp();
    int getInputKey();
}
