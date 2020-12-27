package com.ada.QBSTree;

import com.ada.common.Constants;
import com.ada.geometry.*;
import lombok.Getter;
import lombok.Setter;

import java.util.*;

@Getter
@Setter
public class RCDataNode<T extends ElemRoot> extends RCNode<T> {

	public List<T> elms;

	public RCDataNode() {}

	public RCDataNode(int depth, RCDirNode<T> parent, int position, Rectangle centerRegion, Rectangle region,
                      List<Integer> preDepths, int elemNum, RCtree<T> tree, List<T> elms) {
		super(depth, parent, position, centerRegion,region, preDepths, elemNum, tree);
		this.elms = elms;
	}

	@Override
	public RCDataNode<T> chooseLeafNode(T elem){
		elemNum++;
		elms.add(elem);
		elem.leaf = this;
		if (elem instanceof RectElem)
			updateRegion( ((RectElem) elem).rect,1);
		else
			updateRegion( new Rectangle(new Point(elem.data.clone()), new Point(elem.data.clone())),1);
		return this;
	}

	@Override
	public void chooseLeafForS(T elem, List<RCDataNode<T>> leaves) {
		if (centerRegion.isInternal(elem))
			leaves.add(this);
	}


	@Override
	Rectangle calculateRegion(){
		Rectangle res = null;
		if (this.elemNum != 0) {
			if (this.elms.get(0) instanceof  RectElem){
				List<RectElem> list = (List<RectElem>) elms;
				res = list.get(0).rect.clone();
				for (int i = 1; i < list.size(); i++)
					res = res.getUnionRectangle(list.get(i).rect);
			}else{
				res = new Rectangle(new Point(elms.get(0).data.clone()), new Point(elms.get(0).data.clone()));
				for (int i = 1; i < this.elms.size(); i++)
					res.getUnionRectangle(elms.get(i));
			}
		}
		return res;
	}

	@Override
	void getLeafNodes(List<RCDataNode<T>> list){
		list.add(this);
	}

	@Override
	void getAllElement(List<T> elms) {
		elms.addAll(this.elms);
	}

	void queryLeaf(Rectangle rectangle, List<RCDataNode<T>> leaves) {
		if(region != null && rectangle.isIntersection(region))
			leaves.add(this);
	}

	@Override
	<M extends RectElem> void rectQuery(Rectangle rectangle, List<M> res, boolean isInternal){
		List<M> list = (List<M>) elms;
		if (isInternal){
			for (M m : list) {
				if (rectangle.isInternal(m.rect)) res.add(m);
			}
		}else {
			for (M m : list) {
				if (rectangle.isIntersection(m.rect)) res.add(m);

			}
		}
	}



	boolean insert() {
		if(this.elemNum <= this.tree.upBound) { //没有上溢
			return true;
		}else {//上溢
			RCDirNode<T> UBNode;
			depth = 1;
			convertUpperLayerPD();
			UBNode = getMinReassignNode(false,position);
			RCNode<T> newNode;
			if(UBNode == null) { //没有失衡
				newNode = split();
			}else {  //失衡
				newNode = UBNode.redistribution();
			}
			if(newNode.isRoot()) {
				tree.setRoot(newNode);
			}else {
				newNode.parent.updateUpperLayerDepth();
			}
		}
		return true;
	}




	boolean delete(T elem) {
		RCNode<T> node = this;
		while(node != null) {
			node.elemNum--;
			node = node.parent;
		}
		if(!elms.remove(elem))
			return false;
		if (elem instanceof RectElem)
			updateRegion(((RectElem) elem).rect,2);
		else
			updateRegion(new Rectangle(new Point(elem.data.clone()), new Point(elem.data.clone())),2);
		if(elemNum >= this.tree.lowBound || parent == null) { //没有下溢
			return true;
		}else { //下溢
			RCDirNode<T> UBNode;
			RCNode<T> newNode;
			convertUpperLayerPD();
			if(parent.elemNum > tree.upBound) { //父节点不可合并
				UBNode = parent.getMinReassignNode(false,-1);
				newNode = UBNode.redistribution();
			}else{  //父节点可以合并
				if(parent.isRoot()) { //根节点合并成一个叶节点
					List<T> elms = new ArrayList<>();
					parent.getAllElement(elms);
                    RCDataNode<T> newRoot = new RCDataNode<>(0, null, -1, parent.centerRegion, parent.region,
                            new ArrayList<>(), parent.elemNum, tree, elms);
                    tree.setRoot(newRoot);
                    newRoot.updateElemLeaf();
					return true;
				}
				parent.preDepths.clear();
				parent.preDepths.add(0);
				UBNode = parent.getMinReassignNode(false, position);
				if(UBNode == parent) { //合并父节点就能达到平衡
					List<T> sags = new ArrayList<>();
					UBNode.getAllElement(sags);
					RCDataNode<T> dataNode = new RCDataNode<>(0, UBNode.parent, UBNode.position,
							UBNode.centerRegion, UBNode.region, new ArrayList<>(), UBNode.elemNum, UBNode.tree, sags);
					dataNode.parent.child[dataNode.position] = dataNode;
					dataNode.updateElemLeaf();
					newNode = dataNode;
				}else {
					newNode = UBNode.redistribution();
				}
			}
			if(newNode != null) {
				if (newNode.parent == null)
					tree.setRoot(newNode);
				else
					newNode.parent.updateUpperLayerDepth();
			}
		}
		return true;
	}


	/**
	 * 将叶节点递归的分裂。使用时需要先将该叶节点的parent设置为null，分裂后重新设置parent
	 * @return 返回分裂后的子树的根节点
	 */
	RCDirNode<T> recursionSplit() {
		RCDirNode<T> res;
		res = split();
		for(int childNum = 0; childNum< 4; childNum++) {
			if( ((RCDataNode<T>) res.child[childNum]).elms.size() > tree.upBound )
				((RCDataNode<T>) res.child[childNum]).recursionSplit();
			else {
				if(res.parent != null)
					res.parent.updateUpperLayerDepth();
			}
		}
		return res;
	}

	private RCDirNode<T> split() {
		//本层节点赋值2
		RCDirNode<T> node = new RCDirNode<>(1, parent, position, centerRegion, region, new ArrayList<>(),
				elemNum, tree, new RCNode[4]);
		ElemRoot[] tapElms = elms.toArray(new ElemRoot[0]);
		ElemRoot[][] divide0, divide1, divide2;
		divide0 = binaryDivide(tapElms, 0, tree.precision);
		double bound0 = divide0[1][0].data[0];
		divide1 = binaryDivide(divide0[0], 1, tree.precision);
		double bound10 = divide1[1][0].data[1];
		divide2 = binaryDivide(divide0[1], 1, tree.precision);
		double bound11 = divide2[1][0].data[1];
		ElemRoot[][] divide = new ElemRoot[4][];
		divide[0] = divide1[0];
		divide[1] = divide2[0];
		divide[2] = divide1[1];
		divide[3] = divide2[1];
		Rectangle[] grids = new Rectangle[4];
		grids[0] = new Rectangle(centerRegion.low, new Point(bound0,bound10));
		grids[1] = new Rectangle(new Point(bound0,centerRegion.getLowBound()),
				new Point(centerRegion.getRightBound(),bound11));
		grids[2] = new Rectangle(new Point(centerRegion.getLeftBound(),bound10),
				new Point(bound0,centerRegion.getTopBound()));
		grids[3] = new Rectangle(new Point(bound0,bound11), centerRegion.high);
		for(int chNum = 0; chNum < 4; chNum++) {
			RCDataNode<T> newLeaf = new RCDataNode<>(0, node, chNum, grids[chNum], null, new ArrayList<>(),
					divide[chNum].length, tree, new ArrayList<>(Arrays.asList((T[]) divide[chNum])));
			newLeaf.region = newLeaf.calculateRegion();
			node.child[chNum] = newLeaf;
			newLeaf.updateElemLeaf();
		}
		if(parent != null)
			parent.child[position] = node;
		return node;
	}

	/**
	 * 分堆
	 * @param elems 	被分堆的轨迹段集合
	 * @param depend	依据x坐标分堆传入0， 依据y坐标分堆传入1
	 * @param precision 集合二分点的精度
	 * @return	二维数组表示分堆结果
	 */
	private ElemRoot[][] binaryDivide(ElemRoot[] elems, int depend, int precision){
		ElemRoot[][] res = new ElemRoot[2][];
		int axis, start, end;
		int low = (elems.length*(precision/2))/precision;
		int up = (elems.length*((precision/2)+1))/precision;
		start = 0;
		end = elems.length-1;
		axis = -1;
		while(axis < low || axis > up ) {
			if(axis < low) {
				start = axis + 1;
			}else {
				end = axis - 1;
			}
			choseCandidate(elems, depend, start, end, low);
			axis = divide(elems, start, end, depend);
		}
		res[0] = new ElemRoot[axis];
		res[1] = new ElemRoot[elems.length-axis];
		System.arraycopy(elems, 0, res[0], 0,axis);
		System.arraycopy(elems, axis, res[1], 0,elems.length-axis);
		return res;
	}

	/**
	 *  选择快排候选值，将候选元素同start位置处的元素交换位置
	 */
	private void choseCandidate(ElemRoot[] elems, int depend, int start, int end, int low) {
		int[] candidates = new int[5];
		for(int canNum = 0; canNum<5; canNum++) {
			candidates[canNum] = start + (canNum*(end - start))/4;
		}
		ElemRoot tmp;
		for(int canNum1 = 0; canNum1<4;canNum1++) {
			for(int canNum2 = canNum1+1; canNum2 < 5; canNum2++) {
				if(elems[candidates[canNum1]].data[depend] >
						elems[candidates[canNum2]].data[depend]) {
					tmp = elems[candidates[canNum1]];
					elems[candidates[canNum1]] = elems[candidates[canNum2]];
					elems[candidates[canNum2]] = tmp;
				}
			}
		}
		int compareSiteNum;
		for(compareSiteNum = 0; low > candidates[compareSiteNum]; )
			compareSiteNum++;
		tmp = elems[start];
		elems[start] = elems[candidates[compareSiteNum]];
		elems[candidates[compareSiteNum]] = tmp;
	}

	private int divide(ElemRoot[] elems, int start, int end, int depend) {
		ElemRoot tmp;
		int s = start, e = end;
		boolean direct = true;
		while(s < e) {
			if(direct) {
				while(elems[s].data[depend] <= elems[e].data[depend] && s < e)
					e--;
				tmp = elems[e];
				elems[e] = elems[s];
				elems[s] = tmp;
				if(s < e)
					s++;
				direct = false;
			}else {
				while(elems[s].data[depend] < elems[e].data[depend] && s < e)
					s++;
				tmp = elems[e];
				elems[e] = elems[s];
				elems[s] = tmp;
				if(s < e)
					e--;
				direct = true;
			}
		}
		return s;
	}

	/**
	 * 更新或者初始化本叶子上的全部索引元素的leaf字段。某个索引元素的leaf字段指向该索引元素所在的叶节点
	 */
	private void updateElemLeaf(){
		for (T elem: elms)
			elem.leaf = this;
	}

	@Override
	boolean check(){
		super.check();
		if (!elms.isEmpty() && elms.get(0) instanceof RectElem){
			for (T elem : elms){
				RectElem rectElem = (RectElem) elem;
				if (!region.isInternal(rectElem.rect))
					return false;
				if (!rectElem.rect.getCenter().equals(rectElem))
					return false;
			}
		}

		for (T elem : elms) {
			if (!centerRegion.isInternal(elem))
				return false;
			if (elem.leaf != this)
				return false;
		}
		Rectangle checkRectangle = calculateRegion();
		if (!Rectangle.rectangleEqual(checkRectangle, region))
			return false;
		if (depth != 0)
			return false;
		if (tree.cacheSize <= 0 && elms.size() > tree.upBound)
			return false;
		if (tree.cacheSize <= 0 && !isRoot() && elms.size() < tree.lowBound)
			return false;
		if (elemNum != elms.size())
			return false;
		return true;
	}
}
