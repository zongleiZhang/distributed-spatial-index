package com.ada;

import com.ada.globalTree.GTree;

import java.io.*;
import java.util.*;

public class Test {
    public static void main(String[] args) throws Exception {
        testRedis();
    }

    private static void readInputFile() throws Exception {
        File f = new File("D:\\研究生资料\\论文\\track_data\\成都滴滴\\Sorted_2D\\XY_20161101");
        BufferedReader br = new BufferedReader(new FileReader(f));
//        for (int i = 0; i < 1000; i++) {
//            System.out.println(i + "\t" + br.readLine());
//        }
        int i = 0;
        while (br.readLine() != null){
            i++;
        }
        System.out.println(i);

        br.close();
    }

    private static void testRedis() throws Exception {
        File f = new File("D:\\研究生资料\\论文\\my paper\\MyPaper\\Trajectory\\midData");
        FileInputStream fin = new FileInputStream(f);
        ObjectInputStream ois = new ObjectInputStream(fin);
        int[][] o = (int[][]) ois.readObject();
        ois.close();
        fin.close();
        List<int[][]> list = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            list.add(GTree.redisPatchLeafID(o, 1000 * 12000));
        }
        for (int i = 1; i < list.size(); i++) {
            System.out.println(Arrays.deepEquals(list.get(0), list.get(i)));
        }
        for (int[][] ints : list) {
            int total = 0;
            for (int[] anInt : ints) total += o[anInt[0]][anInt[1]];
            System.out.println(total);
        }
    }
}
