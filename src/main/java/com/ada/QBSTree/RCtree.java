package com.ada.QBSTree;

import com.ada.common.Path;
import com.ada.common.collections.Collections;
import com.ada.geometry.*;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.*;

@Setter
@Getter
public class RCtree<T extends ElemRoot> implements Serializable {

	public RCNode<T> root;

	public int upBound;

	public int lowBound;

	public int balanceFactor;

	public int precision;

	public List<CacheElem> cache;

	public int cacheSize;

	public  RCtree(){}

	/**
	 * initialize the tree
	 * @param lowBound the upper bound of the leaf node's data number.
	 * @param balanceFactor the low bound of the leaf node's data number.
	 */
	public RCtree(int lowBound, int balanceFactor, int precision, Rectangle centerRegion, int cacheSize) {
		this.lowBound = lowBound;
		this.balanceFactor = balanceFactor;
		this.upBound = 5*lowBound;
		this.precision = precision;
		cache = new ArrayList<>();
		this.cacheSize = cacheSize;
		root = new RCDataNode<>(0, null, -1, centerRegion, null, new ArrayList<>(), 0, this, new ArrayList<>());
	}

	public RCtree(int lowBound, int balanceFactor, int precision, Rectangle centerRegion, int cacheSize, List<T> elms) {
		this.lowBound = lowBound;
		this.balanceFactor = balanceFactor;
		this.upBound = 5*lowBound;
		this.precision = precision;
		cache = new ArrayList<>();
		this.cacheSize = cacheSize;
		RCDataNode<T> dataNode = new RCDataNode<>(0, null, -1, centerRegion, null, new ArrayList<>(), elms.size(), this, elms);
		if (elms.isEmpty()){
			root = dataNode;
		}else{
			if (elms.get(0) instanceof RectElem){
				List<RectElem> list = (List<RectElem>) elms;
				List<Rectangle> rectangles = (List<Rectangle>) Collections.changeCollectionElem(list, from -> from.rect);
				Rectangle rect = Rectangle.getUnionRectangle(rectangles.toArray(new Rectangle[0]));
				dataNode.setRegion(rect);
			}else {
				dataNode.setRegion(centerRegion.clone());
			}
			if (dataNode.elemNum > upBound){
				root = dataNode.recursionSplit();
			}else {
				root = dataNode;
			}
		}
	}

	public boolean isEmpty(){
		return root instanceof RCDataNode && root.elemNum == 0;
	}

	private void addToCache(CacheElem elem) {
		cache.add(elem);
		if(cache.size() >= cacheSize)
			clearCache();
	}

	private void clearCache(){
		RCDataNode<T> cur;
		root.depthPreDepthConvert(true);
		//记录需要调整的所有子树
		List<RCNode<T>> adjustNodes = new ArrayList<>();
		for(int caN1 = 0; caN1 < cache.size(); caN1++) {
			cur = cache.get(caN1).leaf;
			RCNode<T> node = cur.getMinReassignNodeForCache();
			Path path = new Path(node);
			//之前加入的子树是新加入的子树的子树，新加入的子树的调整可以覆盖之前加入的子树的调整
			for(int nodeN1 = 0; nodeN1 < adjustNodes.size(); nodeN1++) {
				switch(adjustNodes.get(nodeN1).isSameWay(node) ) {
                    case 0:
                        break;
                    case -1:
                        adjustNodes.remove(nodeN1);
                        nodeN1--;
                        break;
					default:
						throw new IllegalArgumentException("clearCache error.");
				}
			}
			//cache中剩余的元素如果在新加入的子树中，也可以被该子树覆盖
			for(int caN2 = caN1+1; caN2 < cache.size(); caN2++) {
				if(cache.get(caN2).path.isSameWay(path)) {
					cache.remove(caN2);
					caN2--;
				}
			}
			adjustNodes.add(node);
		}

		//记录调整后的节点
		List<RCNode<T>> newNodes = new ArrayList<>();
		for (RCNode<T> adjustNode : adjustNodes) newNodes.add(adjustNode.reAdjust());
		//更新树中各个节点的深度信息
		for (RCNode<T> node : newNodes){
			if (node.parent != null)
				node.parent.updateUpperLayerPreDepth(true,-1);
			else
				root = node;
		}
		root.depthPreDepthConvert(false);
		cache.clear();
	}


	public RCDataNode<T> insert(T elem) {
		if (cacheSize == 0) {
			RCDataNode<T> leafNode = root.chooseLeafNode(elem);
			leafNode.insert();
			return leafNode;
		} else {
			if (root.region == null) {
				if (elem instanceof RectElem)
					root.region = ((RectElem) elem).rect;
				else
					root.region = new Rectangle(new Point(elem.data.clone()), new Point(elem.data.clone()));
			}
			RCDataNode<T> leafNode = root.chooseLeafNode(elem);
			if (leafNode.elemNum > upBound)
				addToCache(new CacheElem(leafNode));
			return leafNode;
		}
	}




	public boolean delete(T elem) {
		try {
			if (cacheSize == 0) {
				RCDataNode<T> leafNode = elem.leaf;
				if (leafNode == null) {
					return false;
				} else {
					elem.leaf = null;
					return leafNode.delete(elem);
				}
			} else {
				RCDataNode<T> leafNode = elem.leaf;
				if (!leafNode.elms.remove(elem))
					return false;
				RCNode<T> node = leafNode;
				while (node != null) {
					node.elemNum--;
					node = node.parent;
				}
				if (elem instanceof RectElem)
					leafNode.updateRegion(((RectElem) elem).rect, 2);
				else {
					leafNode.updateRegion(new Rectangle(new Point(elem.data.clone()), new Point(elem.data.clone())), 2);
				}
				if (leafNode.elemNum < lowBound && leafNode.parent != null) {
					addToCache(new CacheElem(leafNode));
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return true;
	}


	public List<T> pointQuery(Rectangle rectangle) {
		List<T> res = new ArrayList<>();
		List<RCDataNode<T>> leaves = new ArrayList<>();
		root.queryLeaf(rectangle, leaves);
		for (RCDataNode<T> leaf : leaves) {
			for (T elm : leaf.elms) {
				if (rectangle.isInternal(elm))
					res.add(elm);
			}
		}
		return res;
	}

	public <M extends RectElem> List<M> rectQuery(Rectangle rectangle, boolean isInternal) {
		List<M> res = new ArrayList<>();
		root.rectQuery(rectangle, res, isInternal);
		return res;
	}

	
	/**
	 * 查找指定的索引项elem所在的叶节点
	 * @param elem 指定的索引项
	 * @return 索引项elem所在的叶节点
	 */
	public RCDataNode<T> searchLeaf(T elem) {
		RCDataNode<T> leafNode = null;
		List<RCDataNode<T>> leaves = new ArrayList<>();
		root.chooseLeafForS(elem, leaves);
		for (RCDataNode<T> leaf : leaves) {
			if (leaf.elms.contains(elem)) {
				leafNode = leaf;
				break;
			}
		}
		return leafNode;
	}


	public boolean check() {
		return root.check();
	}

    /**
     * 第一次计算root的region
     */
    public void firstRegion() {
	    if (root instanceof RCDataNode){
	        root.region = root.calculateRegion();
        }else {
	        throw new IllegalArgumentException("Error.");
        }
    }

	public void rebuildRoot(Rectangle roodCenterRegion) {
		root.centerRegion = roodCenterRegion;
		cache.clear();
		if (root instanceof  RCDataNode){
			RCDataNode<T> dataNode = (RCDataNode<T>) root;
			if (dataNode.elemNum > upBound)
				root = dataNode.recursionSplit();
		}else{
			RCDirNode<T> dirNode = (RCDirNode<T>) root;
			if (dirNode.elemNum > upBound){
				root = dirNode.redistribution();
			}else{
				List<T> elms = new ArrayList<>();
				dirNode.getAllElement(elms);
				root = new RCDataNode<>(0,null,-1, dirNode.centerRegion, dirNode.region, new ArrayList<>()
						,dirNode.elemNum,dirNode.tree, elms);
			}
		}
	}

	public List<T> getAllElems() {
		List<T> elms = new ArrayList<>(root.elemNum);
		root.getAllElement(elms);
		return elms;
	}
}
