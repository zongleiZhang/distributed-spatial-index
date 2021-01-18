package com.ada.Hausdorff;

import com.ada.common.Constants;
import lombok.Getter;
import lombok.Setter;

import java.util.Objects;

@Getter
@Setter
public class NOAndDistance {
    public int NO;
    public double distance;

    public NOAndDistance(int NO, double distance) {
        this.NO = NO;
        this.distance = distance;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NOAndDistance that = (NOAndDistance) o;
        return NO == that.NO &&
                Constants.isEqual(that.distance, distance);
    }

    @Override
    public int hashCode() {
        return Objects.hash(NO, distance);
    }
}
