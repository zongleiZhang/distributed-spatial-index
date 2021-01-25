package com.ada;

import com.ada.geometry.GridPoint;
import com.ada.geometry.GridRectangle;
import com.ada.globalTree.GDataNode;

import java.util.HashSet;
import java.util.Set;

public class Test {
    public static void main(String[] args) {
        Set<GDataNode> set1 = new HashSet<>();
        Set<GDataNode> set2 = new HashSet<>();
        set1.add(new GDataNode(null, 1, new GridRectangle(new GridPoint(0,0), new GridPoint(5,5)), 0, null, 0));
        set1.add(new GDataNode(null, 1, new GridRectangle(new GridPoint(0,0), new GridPoint(5,5)), 0, null, 1));
        set1.add(new GDataNode(null, 1, new GridRectangle(new GridPoint(0,0), new GridPoint(5,5)), 0, null, 2));
        set1.add(new GDataNode(null, 1, new GridRectangle(new GridPoint(0,0), new GridPoint(5,5)), 0, null, 3));
        set1.add(new GDataNode(null, 1, new GridRectangle(new GridPoint(0,0), new GridPoint(5,5)), 0, null, 4));
        set1.add(new GDataNode(null, 1, new GridRectangle(new GridPoint(0,0), new GridPoint(5,5)), 0, null, 5));
        set2.add(new GDataNode(null, 1, new GridRectangle(new GridPoint(0,0), new GridPoint(5,5)), 0, null, 0));
        set2.add(new GDataNode(null, 1, new GridRectangle(new GridPoint(0,0), new GridPoint(5,5)), 0, null, 1));
        set2.add(new GDataNode(null, 1, new GridRectangle(new GridPoint(0,0), new GridPoint(5,5)), 0, null, 2));
        set2.add(new GDataNode(null, 1, new GridRectangle(new GridPoint(0,0), new GridPoint(5,5)), 0, null, 3));
        set2.add(new GDataNode(null, 1, new GridRectangle(new GridPoint(0,0), new GridPoint(5,5)), 0, null, 4));
        set1.removeAll(set2);
        System.out.println(set1);
    }
}
