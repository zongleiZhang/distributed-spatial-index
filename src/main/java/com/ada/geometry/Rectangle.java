package com.ada.geometry;

import com.ada.common.Constants;
import com.ada.model.globalToLocal.Global2LocalValue;

import java.awt.geom.Line2D;
import java.io.Serializable;
import java.util.List;
import java.util.Objects;

public class Rectangle implements Cloneable, Serializable, Global2LocalValue {
	public Point low; // 左下角的点
	public Point high; // 右上角的点

	public Rectangle() {}

	/**
	 * 构造矩形实例。
	 * @param p1  矩形区域的左下点
	 * @param p2 矩形区域的右上点
	 */
	public Rectangle(Point p1, Point p2) {
		if (p1 == null || p2 == null) // 点对象不能为空
			throw new IllegalArgumentException("Points cannot be null.");
		// 先左下角后右上角
		for (int i = 0; i < 2; i++) {
			if (p1.data[i] > p2.data[i])
				throw new IllegalArgumentException("坐标点为先左下角后右上角");
		}
		low = p1;
		high = p2;
	}

	@Override
	public Rectangle clone() {
		Rectangle rectangle = null;
		try {
			rectangle = (Rectangle) super.clone();
			rectangle.high = new Point(high.data.clone());
			rectangle.low = new Point(low.data.clone());
		} catch (CloneNotSupportedException e) {
			e.printStackTrace();
		}
		return rectangle;
	}

	@Override
	public String toString() {
		return "Low:" + low.toString() + " High:" + high.toString();
	}

	@Override
	public boolean equals(Object o) {
		if (o == null) return false;
		if (this == o) return true;
		if (!(o instanceof Rectangle)) return false;
		Rectangle rectangle = (Rectangle) o;
		return low.equals(rectangle.low) &&
				high.equals(rectangle.high);
	}

	@Override
	public int hashCode() {
		return Objects.hash(low, high);
	}

	public static boolean rectangleEqual(Rectangle curRectangle, Rectangle orgRectangle) {
		if (curRectangle == null && orgRectangle == null)
			return true;
		else if (curRectangle == null || orgRectangle == null)
			return false;
		else
			return curRectangle.low.equals(orgRectangle.low) &&
					curRectangle.high.equals(orgRectangle.high);
	}

	/**
	 * @return 返回左下点
	 */
	public Point getLeftLowPoint() {
		return low;
	}

	/**
	 * @return 返回右上点
	 */
	public Point getRightTopPoint() {
		return high;
	}

	public double getTopBound() {
		return high.data[1];
	}

	public double getRightBound() {
		return high.data[0];
	}

	public double getLowBound() {
		return low.data[1];
	}

	public double getLeftBound() {
		return low.data[0];
	}


	public Point getCenter() {
		return new Point((low.data[0] + high.data[0])/2.0, (low.data[1] + high.data[1])/2.0);
	}


	public Rectangle extendToInt() {
		low.data[0] = Math.floor(low.data[0]);
		low.data[1] = Math.floor(low.data[1]);
		high.data[0] = Math.ceil(high.data[0]);
		high.data[1] = Math.ceil(high.data[1]);
		return this;
	}

	/**
	 * 将矩形长和宽增加times倍
	 */
	public Rectangle extendMultiple(double times) {
		double[] length = new double[low.data.length];
		for(int i = 0; i< length.length; i++) {
			length[i] = (int) ((high.data[i] - low.data[i]) * times);
			if (length[i] < 20.0)
				return extendLength(20.0);
		}
		for(int i = 0; i< length.length; i++) {
			low.data[i] = low.data[i] - length[i] ;
			high.data[i] = high.data[i] + length[i] ;
		}
		return this;
	}

	/**
	 * 将矩形长和宽增加length
	 */
	public Rectangle extendLength(double length) {
		for(int i = 0; i<low.data.length; i++) {
			low.data[i] = low.data[i] - length;
			high.data[i] = high.data[i] + length;
		}
		return this;
	}

	/**
	 * 将本矩形扩展成包含指定矩形rectangle的最小外包矩形
	 */
	public Rectangle getUnionRectangle(Rectangle rectangle) {
		if (rectangle == null) // 矩形不能为空
			throw new IllegalArgumentException("Rectangle cannot be null.");
		double[] min = new double[2];
		double[] max = new double[2];
		for (int i = 0; i < 2; i++) {
			min[i] = Math.min(low.data[i], rectangle.low.data[i]);
			max[i] = Math.max(high.data[i], rectangle.high.data[i]);
		}
		return new Rectangle(new Point(min), new Point(max));
	}

	/**
	 * 调整本矩形为包围原始矩形和指定点的新矩形
	 * @return 发上调整返回true，否则返回false。
	 */
	public boolean getUnionRectangle(Point point) {
		if (point == null)
			throw new IllegalArgumentException("Rectangle cannot be null.");
		boolean[] flag = new boolean[4];
		double tmp;
		tmp = Math.min(low.data[0],point.data[0]);
		flag[0] = (tmp == low.data[0]);
		low.data[0] = tmp;
		tmp = Math.min(low.data[1],point.data[1]);
		flag[1] = (tmp == low.data[1]);
		low.data[1] = tmp;
		tmp = Math.max(high.data[0],point.data[0]);
		flag[2] = (tmp == high.data[0]);
		high.data[0] = tmp;
		tmp = Math.max(high.data[1],point.data[1]);
		flag[3] = (tmp == high.data[1]);
		high.data[1] = tmp;
		return flag[0] || flag[1] || flag[2] || flag[3];
	}

	/**
	 * @return 指定点集的MBR
	 */
	public static Rectangle pointsMBR(Point[] points){
		double xMin = Integer.MAX_VALUE;
		double xMax = Integer.MIN_VALUE;
		double yMin = Integer.MAX_VALUE;
		double yMax = Integer.MIN_VALUE;
		for(Point point:points){
			if (point.data[0] > xMax)
				xMax = point.data[0];
			if (point.data[0] < xMin)
				xMin = point.data[0];
			if (point.data[1] > yMax)
				yMax = point.data[1];
			if (point.data[1] < yMin)
				yMin = point.data[1];
		}
		return new Rectangle(new Point(xMin,yMin),new Point(xMax,yMax));
	}

	/**
	 * @return 包围一系列Rectangle的最小Rectangle
	 */
	public static Rectangle getUnionRectangle(Rectangle[] rectangles) {
		if (rectangles == null || rectangles.length == 0)
			throw new IllegalArgumentException("Rectangle array is empty.");
		Rectangle r0 = rectangles[0].clone();
		for (int i = 1; i < rectangles.length; i++)
			r0 = r0.getUnionRectangle(rectangles[i]); // 获得包裹矩形r0与r[i]的最小边界的矩形再赋值给r0
		return r0; // 返回包围一系列Rectangle的最小Rectangle
	}

	/**
	 * @return 返回Rectangle的面积
	 */
	public double getArea() {
		double area = 1;
		for (int i = 0; i < 2; i++)
			area *= high.data[i] - low.data[i];
		return area;
	}

	/**
	 * 两个Rectangle相交的面积
	 * @param rectangle Rectangle
	 * @return float
	 */
	public double intersectingArea(Rectangle rectangle) {
		if (!isIntersection(rectangle)) // 如果不相交，相交面积为0
			return 0.0;
		double ret = 1.0;
		// 循环一次，得到一个维度的相交的边，累乘多个维度的相交的边，即为面积
		for (int i = 0; i < 2; i++) {
			double l1 = low.data[i];
			double h1 = high.data[i];
			double l2 = rectangle.low.data[i];
			double h2 = rectangle.high.data[i];
			// rectangle1在rectangle2的左边
			if (l1 <= l2 && h1 <= h2) {
				ret *= (h1 - l1) - (l2 - l1);
			}
			// rectangle1在rectangle2的右边
			else if (l1 >= l2 && h1 >= h2) {
				ret *= (h2 - l2) - (l1 - l2);
			}
			// rectangle1在rectangle2里面
			else if (l1 >= l2 && h1 <= h2) {
				ret *= h1 - l1;
			}
			// rectangle1包含rectangle2
			else if (l1 <= l2 && h1 >= h2) {
				ret *= h2 - l2;
			}
		}
		if (ret < Constants.zero)
			return 0.0;
		else
			return ret;
	}

	/**
	 * @return 判断两个Rectangle是否相交
	 */
	public boolean isIntersection(Rectangle rectangle) {
		if (rectangle == null)
			throw new IllegalArgumentException("Rectangle cannot be null.");
		for (int i = 0; i < 2; i++) {
			if ( (low.data[i] - rectangle.high.data[i]) > Constants.zero
					|| (rectangle.low.data[i] - high.data[i]) > Constants.zero )
				return false; // 没有相交
		}
		return true;
	}


	/**
	 * 判断rectangle是否被包围，包括共边。
	 */
	public boolean isInternal(Rectangle rectangle) {
		if (rectangle == null) // 矩形不能为空
			throw new IllegalArgumentException("Rectangle cannot be null.");
		// 只要传入的rectangle有一个维度的坐标越界了就不被包含
		for (int i = 0; i < 2; i++) {
			if ( (low.data[i] - rectangle.low.data[i]) > Constants.zero
					|| (rectangle.high.data[i] - high.data[i]) > Constants.zero)
				return false;
		}
		return true;
	}


	/**
	 * @return 判断矩形是否与线段相交
	 */
	public boolean isIntersection(Segment segment) {
		java.awt.Rectangle jRect = toJRect();
		Line2D.Double line = new Line2D.Double(
				Math.round(segment.p1.data[0]*10000),
				Math.round(segment.p1.data[1]*10000),
				Math.round(segment.p2.data[0]*10000),
				Math.round(segment.p2.data[1]*10000));
		return jRect.intersectsLine(line);
	}

	public java.awt.Rectangle toJRect(){
		int x = (int) Math.round(low.data[0]*10000);
		int y = (int) Math.round(low.data[1]*10000);
		int width = (int) Math.round(high.data[0]*10000)-x;
		int height = (int) Math.round(high.data[1]*10000)-y;
		return new java.awt.Rectangle(x,y,width,height);
	}

	public Rectangle jRectToRectangle(java.awt.Rectangle jRect){
		Point low = new Point(jRect.x/10000.0, jRect.y/10000.0);
		Point high = new Point((jRect.x + jRect.width)/10000.0, (jRect.y + jRect.height)/10000.0);
		return new Rectangle(low,high);
	}


	/**
	 * 判断rectangle与本rectangle是否有重叠的边
	 */
	public boolean isEdgeOverlap(Rectangle rectangle){
		if (isInternal(rectangle)){
			return Constants.isEqual(low.data[0] ,rectangle.low.data[0]) || Constants.isEqual(low.data[1], rectangle.low.data[1]) ||
					Constants.isEqual(high.data[0] ,rectangle.high.data[0]) || Constants.isEqual(high.data[1] ,rectangle.high.data[1]);
		}else
			return false;
	}

	/**
	 * 判断一个点是否在矩形内部,包括边
	 */
	public boolean isInternal(Point point) {
		if (point == null)
			throw new IllegalArgumentException("Rectangle cannot be null.");
		for (int i = 0; i < 2; i++) {
			if ( (low.data[i] - point.data[i]) > Constants.zero
					|| (point.data[i] - high.data[i]) > Constants.zero )
				return false; // 没有相交
		}
		return true;
	}

	/**
	 * 判断一个点是否在矩形内部,包括边
	 */
	public <T extends Point> boolean isInternal(List<T> points) {
		boolean result = true;
		for (Point point : points) {
			if (!isInternal(point)){
				return false;
			}
		}
		return result;
	}

	/**
	 * 判断一个点是否的矩形的边上
	 */
	protected boolean isAtBoundary(Point point){
		if (Constants.isEqual(low.data[0], point.data[0]) || Constants.isEqual(high.data[0], point.data[0]))
			return false;
		return !Constants.isEqual(low.data[1], point.data[1]) && !Constants.isEqual(high.data[1] ,point.data[1]);
	}

	/**
	 * 本Rectangle是一系列Rectangle的MBR，其中一个Rectangle（rectangle1）变化成rectangle2后本Rectangle是否会发生变化
	 * @param rectangle1 变化前的Rectangle
	 * @param rectangle2 变化后的Rectangle
	 * @return 发生变化返回true，否则返回false
	 */
	public boolean isMBRChange(Rectangle rectangle1, Rectangle rectangle2) {
		boolean flag = false;
		if ( Constants.isEqual(this.getTopBound(), rectangle1.getTopBound())
				&& !Constants.isEqual(this.getTopBound() ,rectangle2.getTopBound()) )
			flag = true;
		if ( Constants.isEqual(this.getLowBound(), rectangle1.getLowBound())
				&& !Constants.isEqual(this.getLowBound(), rectangle2.getLowBound()) )
			flag = true;
		if ( Constants.isEqual(this.getLeftBound() ,rectangle1.getLeftBound())
				&& !Constants.isEqual(this.getLeftBound() ,rectangle2.getLeftBound()))
			flag = true;
		if ( Constants.isEqual(this.getRightBound() ,rectangle1.getRightBound())
				&& !Constants.isEqual(this.getRightBound() ,rectangle2.getRightBound()))
			flag = true;
		if (!flag)
			flag = !isInternal(rectangle2);
		return flag;
	}

	/**
	 *
	 */
    public Rectangle createIntersection(Rectangle rectangle) {
		if (!isIntersection(rectangle))
			throw new IllegalArgumentException("Cut to internal error");
		java.awt.Rectangle jDRect0 = toJRect();
		java.awt.Rectangle jDRect1 = rectangle.toJRect();
		java.awt.Rectangle jDRect = (java.awt.Rectangle) jDRect0.createIntersection(jDRect1);
		return jRectToRectangle(jDRect);
    }


	/**
	 * 本矩形与全局矩形有一些共同的边，将这些边扩展到足够大
	 */
	public Rectangle extendToEnoughBig() {
		Rectangle newRect = this.clone();
		double tmp = (Constants.globalRegion.high.data[0] - Constants.globalRegion.low.data[0])*1.5;
		if (Constants.isEqual(low.data[0],Constants.globalRegion.low.data[0]))
			newRect.low.data[0] -= tmp;
		if (Constants.isEqual(low.data[1],Constants.globalRegion.low.data[1]))
			newRect.low.data[1] -= tmp;
		if (Constants.isEqual(high.data[0],Constants.globalRegion.high.data[0]))
			newRect.high.data[0] += tmp;
		if (Constants.isEqual(high.data[1],Constants.globalRegion.high.data[1]))
			newRect.high.data[1] += tmp;
		return newRect;
	}

}
