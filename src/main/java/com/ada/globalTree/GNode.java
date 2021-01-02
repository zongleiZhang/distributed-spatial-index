package com.ada.globalTree;

import com.ada.geometry.GridPoint;
import com.ada.geometry.GridRectangle;
import com.ada.geometry.GLeafAndBound;
import com.ada.geometry.Rectangle;
import com.ada.geometry.TrackKeyTID;
import org.roaringbitmap.RoaringBitmap;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@Getter
@Setter
public abstract class GNode implements Serializable {

    public int elemNum;

    transient public GDirNode parent;

    transient public int position;

    public GridRectangle gridRegion;

    public Rectangle region;

    public RoaringBitmap bitmap;

    transient public GTree tree;

    public GNode(){}

    public GNode(GDirNode parent, int position, GridRectangle gridRegion, int elemNum, GTree tree) {
        this.parent = parent;
        this.position = position;
        this.gridRegion = gridRegion;
        this.region = gridRegion.toRectangle();
        this.elemNum = elemNum;
        bitmap = new RoaringBitmap();
        this.tree = tree;
    }

    public boolean check(Map<Integer, TrackKeyTID> trackMap) {
        int total = tree.getRangeEleNum(gridRegion);
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
        if (!gridRegion.toRectangle().equals(region))
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


    public boolean isRoot(){
        return parent == null;
    }

    /**
     * 更新本节点的elemNum成员，并返回新的值。
     */
    abstract int updateElemNum();

    /**
     * 统计当前子树的所有叶节点
     */
    public abstract void getLeafs(List<GDataNode> leafs);

    /**
     * 查看GridPoint点gPoint属于哪个叶节点
     */
    public abstract GDataNode searchGPoint(GridPoint gPoint);

    /**
     * 查找本子树与rectangle相交的叶节点，将叶节点存储在leafs中
     */
    public abstract void getIntersectLeafNodes(Rectangle rectangle, List<GDataNode> leafs);

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GNode gNode = (GNode) o;
        if (elemNum != gNode.elemNum)
            return false;
        if (!Objects.equals(gridRegion, gNode.gridRegion))
            return false;
        if (!Objects.equals(region, gNode.region))
            return false;
        return true;
    }

    public abstract GNode getInternalNode(Rectangle rectangle);

    public abstract void getAllDirNode(List<GDirNode> dirNodes);
}
