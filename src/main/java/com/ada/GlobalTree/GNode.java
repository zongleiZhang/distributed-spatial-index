package com.ada.GlobalTree;

import com.ada.geometry.GridPoint;
import com.ada.geometry.GridRectangle;
import com.ada.geometry.Rectangle;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

@Getter
@Setter
public abstract class GNode implements Serializable {

    transient public int elemNum;

    transient public GDirNode parent;

    transient public int position;

    public GridRectangle gridRegion;

    public Rectangle region;

    transient public GTree tree;

    public GNode(){}

    public GNode(GDirNode parent, int position, GridRectangle gridRegion, int elemNum, GTree tree) {
        this.parent = parent;
        this.position = position;
        this.gridRegion = gridRegion;
        this.region = gridRegion.toRectangle();
        this.elemNum = elemNum;
        this.tree = tree;
    }

    public boolean check() {
        int total = 0;
        for (int i = gridRegion.low.x; i <= gridRegion.high.x; i++) {
            for (int j = gridRegion.low.y; j <= gridRegion.high.y; j++)
                total += tree.density[i][j];
        }
        if (elemNum != total)
            throw new IllegalArgumentException("elemNum error");
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
                ((GDirNode ) this).child[chNum].check();
        }
        return true;
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
     * 返回当前子树的所有叶节点
     */
    public abstract List<GDataNode> getLeafs();

    /**
     * 查看GridPoint点gPoint属于哪个叶节点
     */
    public abstract GDataNode searchGPoint(GridPoint gPoint);

    /**
     * 查找本子树与rectangle相交的叶节点，将叶节点存储在leafs中
     */
    public abstract void getIntersectLeafNodes(Rectangle rectangle, List<GDataNode> leafs);

    /**
     * 填充中间节点的leafs成员
     */
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
}
