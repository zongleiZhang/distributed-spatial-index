package com.ada.Xie_function.STRTree;

import com.ada.common.Constants;
import com.ada.common.collections.Collections;
import com.ada.geometry.Rectangle;
import com.ada.geometry.TrackPoint;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class STRTree implements Serializable {

    private STRNode root;

    public STRTree(List<TrackPoint> points){
        points.sort((o1, o2) -> Double.compare(o2.data[1], o1.data[1]));
        int rowNum = (int) Math.sqrt(Constants.dividePartition);
        List<List<TrackPoint>> lists = new ArrayList<>(rowNum);
        Rectangle[] regions = new Rectangle[rowNum];
        Rectangle gr = Constants.globalRegion;
        double upside = gr.getTopBound();
        List<TrackPoint> list;
        for (int i = 0; i < rowNum-1; i++) {
            list = points.subList((int)((i/ (double) rowNum)*points.size()), (int)(((i+1)/ (double) rowNum)*points.size()));
            lists.add(list);
            double bottom = points.get((int)(((i+1)/ (double) rowNum)*points.size())).data[1];
            regions[i] = new Rectangle(upside, bottom, gr.getLeftBound(), gr.getRightBound());
            upside = bottom;
        }
        list = points.subList((int)(((rowNum-1)/ (double) rowNum)*points.size()), points.size());
        lists.add(list);
        regions[rowNum-1] = new Rectangle(upside, gr.getLowBound(), gr.getLeftBound(), gr.getRightBound());
        List<LeafNode> leaves = new ArrayList<>(Constants.dividePartition);
        int colNum = Constants.dividePartition/rowNum;
        int mod = Constants.dividePartition%rowNum;
        int i = 0;
        for (; i < mod; i++) {
            lists.get(i).sort(Comparator.comparingDouble(o -> o.data[0]));
            double left = gr.getLeftBound();
            for (int j = 0; j < colNum; j++) {
                double right = lists.get(i).get((int)(((j+1)/(colNum+1.0))*lists.get(i).size())).data[0];
                leaves.add(new LeafNode(new Rectangle( regions[i].getTopBound(), regions[i].getLowBound(), left, right)));
                left = right;
            }
            leaves.add(new LeafNode(new Rectangle( regions[i].getTopBound(), regions[i].getLowBound(), left, gr.getRightBound())));
        }
        for (; i < rowNum; i++) {
            lists.get(i).sort(Comparator.comparingDouble(o -> o.data[0]));
            double left = gr.getLeftBound();
            for (int j = 0; j < colNum-1; j++) {
                double right = lists.get(i).get((int)(((j+1.0)/colNum)*lists.get(i).size())).data[0];
                leaves.add(new LeafNode(new Rectangle( regions[i].getTopBound(), regions[i].getLowBound(), left, right)));
                left = right;
            }
            leaves.add(new LeafNode(new Rectangle( regions[i].getTopBound(), regions[i].getLowBound(), left, gr.getRightBound())));
        }
        for (int j = 0; j < leaves.size(); j++)
            leaves.get(j).leafID = j;
        root = STRPackage(leaves).get(0);
    }

    @SuppressWarnings("unchecked")
    private <T extends STRNode> List<T> STRPackage(List<T> nodes){
        List<T> packageNodes = new ArrayList<>();
        if (nodes.size() <= 8) {
            rowPackage(nodes, packageNodes);
            if (packageNodes.size() == 1){
                return packageNodes;
            }else {
                DirNode dirNode = new DirNode(null, new ArrayList<>(4));
                dirNode.children.addAll(packageNodes);
                Rectangle[] rects = Collections.changeCollectionElem(dirNode.children, t -> t.region).toArray(new Rectangle[0]);
                dirNode.region = Rectangle.getUnionRectangle(rects);
                return (List<T>) java.util.Collections.singletonList(dirNode);
            }
        }else {
            int nodeNum = (int)Math.ceil(nodes.size()/4.0);
            int rowNum = (int) Math.sqrt(nodeNum);
            nodes.sort((o1, o2) -> Double.compare(o2.region.getCenter().data[1], o1.region.getCenter().data[1]));
            List<List<T>> lists = new ArrayList<>(rowNum);
            List<T> list;
            for (int i = 0; i < rowNum-1; i++) {
                list = nodes.subList((int)((i/ (double) rowNum)*nodes.size()), (int)(((i+1)/ (double) rowNum)*nodes.size()));
                lists.add(list);
            }
            list = nodes.subList((int)(((rowNum-1)/ (double) rowNum)*nodes.size()), nodes.size());
            lists.add(list);
            for (int i = 0; i < rowNum; i++) {
                rowPackage(lists.get(i), packageNodes);
            }
            return STRPackage(packageNodes);
        }
    }

    @SuppressWarnings("unchecked")
    private <T extends STRNode> void rowPackage(List<T> nodes, List<T> result) {
        nodes.sort(Comparator.comparingDouble(o -> o.region.getCenter().data[1]));
        DirNode dirNode = new DirNode(null, new ArrayList<>(4));
        dirNode.children.add(nodes.get(0));
        for (int i = 1; i < nodes.size(); i++) {
            if(i%4 == 0){
                Rectangle[] rects = Collections.changeCollectionElem(dirNode.children, t -> t.region).toArray(new Rectangle[0]);
                dirNode.region = Rectangle.getUnionRectangle(rects);
                result.add((T) dirNode);
                dirNode = new DirNode(null, new ArrayList<>(4));
            }
            dirNode.children.add(nodes.get(i));
        }
        if (dirNode.children.size() != 0){
            Rectangle[] rects = Collections.changeCollectionElem(dirNode.children, t -> t.region).toArray(new Rectangle[0]);
            dirNode.region = Rectangle.getUnionRectangle(rects);
            result.add((T) dirNode);
        }
    }

    public List<Integer> searchLeafIDs(Rectangle rectangle){
        List<LeafNode> result = new ArrayList<>();
        root.search(rectangle, result);
        return (List<Integer>) Collections.changeCollectionElem(result, leaf -> leaf.leafID);
    }

    public void check() {
        if (!root.check())
            throw new IllegalArgumentException("check error.");
    }
}


















