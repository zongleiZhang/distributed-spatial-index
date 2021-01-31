package com.ada.model.densityToGlobal;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;

@Getter
@Setter
public class Density implements D2GElem, Serializable {
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
    public int getD2GKey() {
        return key;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Density density = (Density) o;
        return fromKey == density.fromKey &&
                Arrays.deepEquals(grids, density.grids);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(fromKey);
        result = 31 * result + Arrays.hashCode(grids);
        return result;
    }
}
