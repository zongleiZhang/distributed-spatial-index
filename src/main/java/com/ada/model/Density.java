package com.ada.model;

import com.ada.common.Constants;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;

@Getter
@Setter
public class Density implements DensityToGlobalElem, Serializable {
    public int[][] grids;
    public int key;
    public int fromKey;

    public Density() {}

    public Density(int[][] grids, int key, int fromKey) {
        this.grids = grids;
        this.key = key;
        this.fromKey = fromKey;
    }

    @Override
    public Integer getDensityToGlobalKey() {
        return key;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Density density = (Density) o;
        return fromKey == density.fromKey &&
                Constants.arrsEqual(grids, density.grids);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(fromKey);
        result = 31 * result + Arrays.hashCode(grids);
        return result;
    }
}
