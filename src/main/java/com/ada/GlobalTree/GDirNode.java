package com.ada.GlobalTree;

import com.ada.Grid.GridPoint;
import com.ada.Grid.GridRectangle;
import com.ada.trackSimilar.Rectangle;

import java.util.List;

public class GDirNode extends GNode{

    public GNode[] child;

    public List<GDataNode> leafs;

    public GDirNode(){}

    public GDirNode(GDirNode parent, int position, GridRectangle centerRegion, int elemNum, GTree tree, GNode[] child) {
        super(parent, position, centerRegion, elemNum, tree);
        this.child = child;
    }

    public List<GDataNode> getLeafs() {
        return leafs;
    }

    public void setLeafs(List<GDataNode> leafs) {
        this.leafs = leafs;
    }

    boolean checkGDirNode() {
        if (elemNum != child[0].elemNum + child[1].elemNum + child[2].elemNum + child[3].elemNum)
            throw new IllegalArgumentException("elemNum error");
        if ( region.low.y != child[0].region.low.y ||
                child[0].region.high.y+1 != child[1].region.low.y ||
                child[1].region.high.y != region.high.y ||
                region.low.y != child[2].region.low.y ||
                child[2].region.high.y+1 != child[3].region.low.y ||
                child[3].region.high.y != region.high.y ||
                region.low.x != child[0].region.low.x ||
                region.low.x != child[1].region.low.x ||
                region.high.x != child[2].region.high.x ||
                region.high.x != child[3].region.high.x ||
                child[0].region.high.x != child[1].region.high.x ||
                child[0].region.high.x+1 != child[2].region.low.x ||
                child[0].region.high.x+1 != child[3].region.low.x)
            throw new IllegalArgumentException("region error");
        return true;
    }

    public GNode[] getChild() {
        return child;
    }

    public void setChild(GNode[] child) {
        this.child = child;
    }

    public GDataNode searchGPoint(GridPoint gPoint) {
        for (GNode gNode : child) {
            if (gNode.region.isInternal(gPoint))
                return gNode.searchGPoint(gPoint);
        }
        return null;
    }

    void getIntersectLeafIDs(Rectangle rectangle, List<Integer> leafIDs) {
        for (GNode node : child) {
            if (rectangle.isIntersection(node.rectangle)) {
                if (rectangle.isInternal(node.rectangle))
                    node.getAllLeafID(leafIDs);
                else
                    node.getIntersectLeafIDs(rectangle, leafIDs);
            }
        }
    }

    public void getIntersectLeafNodes(Rectangle rectangle, List<GDataNode> leafs) {
        for (GNode node : child) {
            if (rectangle.isIntersection(node.rectangle)) {
                if (rectangle.isInternal(node.rectangle))
                    leafs.addAll(node.getLeafs());
                else
                    node.getIntersectLeafNodes(rectangle, leafs);
            }
        }
    }

    public GNode getInternalNode(Rectangle rectangle){
        for (GNode gNode : child) {
            if (gNode.rectangle.isInternal(rectangle))
                return gNode.getInternalNode(rectangle);
        }
        return this;
    }

    public void getAllDirNode(List<GDirNode> dirNodes){
        dirNodes.add(this);
        for (GNode gNode : child)
            gNode.getAllDirNode(dirNodes);
    }


    void setAllElemNumZero() {
        elemNum = 0;
        for (GNode gNode : child)
            gNode.setAllElemNumZero();
    }

    void alterLeafs(List<GDataNode> oldLeafs, List<GDataNode> newLeafs) {
        leafs.removeAll(oldLeafs);
        leafs.addAll(newLeafs);
        if (parent != null)
            parent.alterLeafs(oldLeafs, newLeafs);
    }
}
