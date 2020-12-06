package com.ada.QBSTree;

import com.ada.geometry.Point;
import com.ada.geometry.Rectangle;

import java.util.Objects;

public class Elem extends Rectangle{
	public Point center;
	public RCDataNode leaf;

	public Elem() {	}

	public Elem(Point low, Point high) {
		Point tmp;
		if (low.data[0] > high.data[0]){
			tmp = low;
			low = high;
			high = tmp;
		}
		if (low.data[1] > high.data[1]){
			this.low = new Point(low.data[0],high.data[1]);
			this.high = new Point(high.data[0],low.data[1]);
		}else {
			this.low = low;
			this.high = high;
		}
		center = new Point((this.low.data[0] + this.high.data[0]) / 2,
				(this.low.data[1] + this.high.data[1]) / 2);
	}

	public boolean check(){
		if (!leaf.elms.contains(this))
			return false;
		RCNode node = leaf;
		while (!node.isRoot()){
			if (node.parent.child[node.position] != node)
				return false;
			node = node.parent;
		}
		return leaf.tree.root == node;

	}

	@Override
	public Elem clone() {
		Elem elem = (Elem) super.clone();
		elem.center = center.clone();
		return elem;
	}


	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (!(o instanceof Elem)) return false;
		if (!super.equals(o)) return false;
		Elem elem = (Elem) o;
		return center.equals(elem.center);
	}

	@Override
	public int hashCode() {
		return Objects.hash(super.hashCode(), center);
	}

//	@Override
//	public boolean getUnionRectangle(Point point) {
//		if (super.getUnionRectangle(point)){
//			center.data[0] = (low.data[0]+high.data[0])/2;
//			center.data[1] = (low.data[1]+high.data[1])/2;
//		}
//		return true;
//	}
}