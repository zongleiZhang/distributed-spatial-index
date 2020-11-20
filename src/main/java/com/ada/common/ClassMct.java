package com.ada.common;

/**
 * 墨卡托投影
 */
public class ClassMct {
    static public int __IterativeTimes = 10; //反向转换程序中的迭代次数
    static public double __IterativeValue = 0; //反向转换程序中的迭代初始值
    static public double __A = 6378.137; //椭球体长轴,千米
    static public double __B = 6356.752314; //椭球体短轴,千米
    static public double __B0 = 0.5235987755982988; //标准纬度,弧度
    static public double __L0 = 1.8151424220741026; //原点经度,弧度

    //设定__B0
    static public void SetLB0(double pmtL0, double pmtB0)
    {
        double l0 = Math.toRadians(pmtL0);
        if (l0 < -Math.PI || l0 > Math.PI)
        {
            return;
        }
        __L0 = l0;

        double b0 = Math.toRadians(pmtB0);
        if (b0 < -Math.PI / 2 || b0 > Math.PI / 2)
        {
            return;
        }
        __B0 = b0;
    }

    /**
     * 经纬度转XY坐标
     * @param lat 纬度
     * @param lon 经度
     * @return 直角坐标，单位：米。 double[0] x, double[1] y。
     */
    static public double[] LBToXY( double lat, double lon)
    {
        double B = Math.toRadians(lat);
        double L = Math.toRadians(lon);

        double[] result = new double[2];
        result[0] = 0.0;
        result[1] = 0.0;
        double[] res = new double[2];

        double f/*扁率*/, e/*第一偏心率*/, e_/*第二偏心率*/, NB0/*卯酉圈曲率半径*/, K, dtemp;
        double E = Math.exp(1);
        if (L < -Math.PI || L > Math.PI || B < -Math.PI / 2 || B > Math.PI / 2)
        {
            result[0] *= 1000.0;
            result[1] *= -1000.0;
            res[0] = result[0];
            res[1] = result[1];
            return res;
        }
        if (__A <= 0 || __B <= 0)
        {
            result[0] *= 1000.0;
            result[1] *= -1000.0;
            res[0] = result[0];
            res[1] = result[1];
            return res;
        }
        f = (__A - __B) / __A;
        dtemp = 1 - (__B / __A) * (__B / __A);
        if (dtemp < 0)
        {
            result[0] *= 1000.0;
            result[1] *= -1000.0;
            res[0] = result[0];
            res[1] = result[1];
            return res;
        }
        e = Math.sqrt(dtemp);
        dtemp = (__A / __B) * (__A / __B) - 1;
        if (dtemp < 0)
        {
            result[0] *= 1000.0;
            result[1] *= -1000.0;
            res[0] = result[0];
            res[1] = result[1];
            return res;
        }
        e_ = Math.sqrt(dtemp);
        NB0 = ((__A * __A) / __B) / Math.sqrt(1 + e_ * e_ * Math.cos(__B0) * Math.cos(__B0));
        K = NB0 * Math.cos(__B0);
        result[0] = K * (L - __L0);
        result[1] = K * Math.log(Math.tan(Math.PI / 4 + (B) / 2) * Math.pow((1 - e * Math.sin(B)) / (1 + e * Math.sin(B)), e / 2));
        double y0 = K * Math.log(Math.tan(Math.PI / 4 + (__B0) / 2) * Math.pow((1 - e * Math.sin(__B0)) / (1 + e * Math.sin(__B0)), e / 2));
        result[1] = result[1] - y0;
        result[1] = -result[1];//正常的Y坐标系（向上）转程序的Y坐标系（向下）
        result[0] *= 1000.0;
        result[1] *= -1000.0;
        res[0] = result[0];
        res[1] = result[1];
        return res;
    }
}
