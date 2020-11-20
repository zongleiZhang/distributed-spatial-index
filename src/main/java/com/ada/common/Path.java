package com.ada.common;

import com.ada.GlobalTree.GDataNode;
import com.ada.GlobalTree.GDirNode;
import com.ada.GlobalTree.GNode;
import com.ada.QBSTree.RCNode;

import java.util.ArrayList;
import java.util.List;

public class Path {
    public long way;

    public Path(){}

    public Path(long way){
        this.way = way;
    }

    public Path(GNode node){
        setWay(node);
    }

    public Path(RCNode node){
        setWay(node);
    }

    public void setWay(RCNode node){
        long tmp;
        if(node.parent != null) {
            way =  (long) ((node.position << 1) + 1);
            node = node.parent;
        }
        while(true) {
            if(node.parent != null) {
                tmp = (long) ((node.position << 1) + 1);
                way = way << (2+1) ;
                way += tmp;
            }else
                break;
            node = node.parent;
        }
    }

    public void setWay(GNode node){
        long tmp;
        if(node.parent != null) {
            way =  (long) ((node.position << 1) + 1);
            node = node.parent;
        }
        while(true) {
            if(node.parent != null) {
                tmp = (long) ((node.position << 1) + 1);
                way = way << (2+1) ;
                way += tmp;
            }else
                break;
            node = node.parent;
        }
    }

    public List<Integer> getIntsPath(){
        long path = this.way;
        List<Integer> ps = new ArrayList<>();
        int position;
        position = (int) path%8;
        path = path >> 3;
        while (position != 0){
            position = position >> 1;
            ps.add(position);
            position = (int) path%8;
            path = path >> 3;
        }
        return ps;
    }

    /**
     * 根据路径获取树node的相应节点
     * @param root 树node
     * @return 相应的节点
     */
    public GNode getNode(GNode root){
        if (root instanceof GDataNode) {
            if (root.parent == null && way == 0)
                return root;
            throw new IllegalArgumentException("error");
        }
        List<Integer> path;
        path = getIntsPath();
        GNode node1 = root;
        for (Integer integer:path){
            if ( node1 instanceof GDataNode)
                throw new IllegalArgumentException("error");
            node1 = ((GDirNode) node1).child[integer];
        }
        return node1;
    }

    /**
     * 判断其中一条路径是否是另一条路径的子路径
     * @param path0 路径0
     * @param path1 路径1
     * @return 0：两条路径互相没有自路径，1：path0是path1的子路径， -1： path1是path0的子路径
     */
    public static int isSameWay(Path path0,Path path1){
        List<Integer> list0 = path0.getIntsPath();
        List<Integer> list1 = path1.getIntsPath();
        List<Integer> tmp;
        boolean zeroBigger = true;
        if (list0.size() < list1.size()){
            zeroBigger = false;
            tmp = list0;
            list0 = list1;
            list1 = tmp;
        }
        for (int i = 0; i < list1.size(); i++) {
            if (!list0.get(i).equals(list1.get(i))){
                return 0;
            }
        }
        if (zeroBigger)
            return -1;
        else
            return 1;
    }

    public boolean isSameWay(Path path){
        boolean res = true;
        List<Integer> list1 = getIntsPath();
        List<Integer> list2 = path.getIntsPath();
        int size = Math.min(list1.size(),list2.size());
        for (int i = 0; i < size; i++) {
            if (!list1.get(i).equals(list2.get(i))) {
                res = false;
                break;
            }
        }
        return res;
    }

    @Override
    public String toString() {
        return getIntsPath().toString();
    }
}
