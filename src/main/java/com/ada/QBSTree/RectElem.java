package com.ada.QBSTree;

import com.ada.trackSimilar.Point;
import com.ada.trackSimilar.Rectangle;

public class RectElem extends ElemRoot{

    public Rectangle rect;

    public RectElem() { }

    public RectElem(RCDataNode leaf, double[] data, Rectangle rectangle) {
        super(leaf, data);
        this.rect = rectangle;
    }

    /**
     * 用索引矩形的两个对角顶点初始化一个索引对象，这两个点可以不计顺序
     */
    public RectElem(Point p1, Point p2) {
        Point tmp;
        if (p1.data[0] > p2.data[0]){
            tmp = p1;
            p1 = p2;
            p2 = tmp;
        }
        Point pLow, pHigh;
        if (p1.data[1] > p2.data[1]){
            pLow = new Point(p1.data[0], p2.data[1]);
            pHigh = new Point(p2.data[0], p1.data[1]);
        }else {
            pLow = p1;
            pHigh = p2;
        }
        rect = new Rectangle(pLow, pHigh);
        data = new double[]{(pLow.data[0] + pHigh.data[0]) / 2,
                (pLow.data[1] + pHigh.data[1]) / 2};

    }

    public Rectangle getRect() {
        return rect;
    }

    public void setRect(Rectangle rect) {
        this.rect = rect;
    }

    @Override
    public RectElem clone() {
        RectElem rectElem = (RectElem) super.clone();
        rectElem.rect = rect.clone();
        return rectElem;
    }
}
