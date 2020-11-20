package com.ada.trackSimilar;

import com.ada.GlobalTree.GDataNode;
import org.jetbrains.annotations.NotNull;

import java.util.Objects;

public class GLeafAndBound implements Comparable<GLeafAndBound>{
    public GDataNode leaf;
    public double bound;

    public GLeafAndBound() {
    }

    public GLeafAndBound(GDataNode leaf, double bound) {
        this.leaf = leaf;
        this.bound = bound;
    }


    @Override
    public int compareTo(@NotNull GLeafAndBound o) {
        return Double.compare(bound, o.bound);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GLeafAndBound that = (GLeafAndBound) o;
        return leaf.equals(that.leaf);
    }

    @Override
    public int hashCode() {
        return Objects.hash(leaf);
    }
}
