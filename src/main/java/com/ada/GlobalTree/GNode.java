package com.ada.GlobalTree;

import com.ada.Grid.GridPoint;
import com.ada.Grid.GridRectangle;
import com.ada.trackSimilar.GLeafAndBound;
import com.ada.trackSimilar.Rectangle;
import com.ada.trackSimilar.TrackKeyTID;
import org.roaringbitmap.RoaringBitmap;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public abstract class GNode implements Serializable {

    transient public int elemNum;

    transient public GDirNode parent;

    transient public int position;

    public GridRectangle region;

    public Rectangle rectangle;

    public RoaringBitmap bitmap;

    transient public GTree tree;

    public boolean check(Map<Integer, TrackKeyTID> trackMap) {
        int total = 0;
        for (int i = region.low.x; i <= region.high.x; i++) {
            for (int j = region.low.y; j <= region.high.y; j++)
                total += tree.density[i][j];
        }
        if (elemNum != total)
            throw new IllegalArgumentException("elemNum error");
        for (Integer TID : bitmap) {
            TrackKeyTID track = trackMap.get(TID);
            if (this instanceof GDataNode){
                GDataNode dataNode = (GDataNode) this;
                GLeafAndBound gb = new GLeafAndBound(dataNode, 0.0);
                if (track.enlargeTuple.f0 != dataNode &&
                        !track.passP.contains(dataNode) &&
                        (track.topKP.isEmpty() || !track.topKP.getList().contains(gb)))
                    throw new IllegalArgumentException("bitmap error " + TID);
            }else {
                if (track.enlargeTuple.f0 != this)
                    throw new IllegalArgumentException("track.enlargeTuple.f0 != this " + TID);
            }
        }
        if (!region.toRectangle().equals(rectangle))
            throw new IllegalArgumentException("rectangle error");
        if(!isRoot()) {
            if (parent.child[position] != this)
                throw new IllegalArgumentException("parent child node error");
        }

        if (this instanceof GDirNode) {
            if (!((GDirNode) this).checkGDirNode())
                return false;
            for(int chNum = 0; chNum<4; chNum++)
                ((GDirNode ) this).child[chNum].check(trackMap);
        }
        return true;
    }

    public GTree getTree() {
        return tree;
    }

    public void setTree(GTree tree) {
        this.tree = tree;
    }

    public GNode(){}

    public GNode(GDirNode parent, int position, GridRectangle region, int elemNum, GTree tree) {
        this.parent = parent;
        this.position = position;
        this.region = region;
        this.rectangle = region.toRectangle();
        this.elemNum = elemNum;
        bitmap = new RoaringBitmap();
        this.tree = tree;
    }

    public int getElemNum() {
        return elemNum;
    }

    public void setElemNum(int elemNum) {
        this.elemNum = elemNum;
    }

    public GDirNode getParent() {
        return parent;
    }

    public void setParent(GDirNode parent) {
        this.parent = parent;
    }

    public int getPosition() {
        return position;
    }

    public void setPosition(int position) {
        this.position = position;
    }

    public GridRectangle getRegion() {
        return region;
    }

    public void setRegion(GridRectangle region) {
        this.region = region;
    }

    /**
     * 将GlobalTree中的所有节点的elemNum清零
     */
    abstract void setAllElemNumZero();


    public boolean isRoot(){
        return parent == null;
    }

    /**
     * 从叶节点开始更新祖先节点的elemNum
     * @param elemNum 叶节点索引项数目
     */
    void updateLeafElemNum(int elemNum) {
        this.elemNum += elemNum;
        if (parent != null)
            parent.updateLeafElemNum(elemNum);
    }


    /**
     * 获取当前子树的所有叶节点ID添加到newLeafNodes中
     */
    void getAllLeafID(List<Integer> leafIDs){
        for (GDataNode leaf : getLeafs())
            leafIDs.add(leaf.leafID);
    }

    /**
     * 返回当前子树的所有叶节点
     */
    public abstract List<GDataNode> getLeafs();

    /**
     * 查看GridPoint点gPoint属于哪个叶节点
     */
    public abstract GDataNode searchGPoint(GridPoint gPoint);

    /**
     * 查找本子树与GridRectangle矩形gRectangle相交的叶节点，将leafID存储在leafs中
     */
    abstract void getIntersectLeafIDs(Rectangle rectangle, List<Integer> leafIDs);

    /**
     * 查找本子树与rectangle相交的叶节点，将叶节点存储在leafs中
     */
    public abstract void getIntersectLeafNodes(Rectangle rectangle, List<GDataNode> leafs);

    public abstract GNode getInternalNode(Rectangle rectangle);

    void countLeafs(){
        if (this instanceof GDirNode){
            GDirNode dirNode = (GDirNode) this;
            dirNode.leafs = new ArrayList<>();
            for (GNode gNode : dirNode.child) {
                if (gNode instanceof GDirNode){
                    gNode.countLeafs();
                    dirNode.leafs.addAll(((GDirNode) gNode).leafs);
                }else {
                    dirNode.leafs.add((GDataNode) gNode);
                }
            }
        }else {
            throw new IllegalArgumentException("error count leafs.");

        }
    }


    public abstract void getAllDirNode(List<GDirNode> dirNodes);
}
