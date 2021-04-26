package com.ada;

import com.ada.globalTree.GTree;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Test {
    public static void main(String[] args) throws Exception {
//        BufferedReader br = new BufferedReader(new FileReader(Constants.dataSingleFileName));
//        BufferedWriter bw = new BufferedWriter(new FileWriter("D:\\研究生资料\\track_data\\成都滴滴\\Experiment\\Single"));
//        String str = br.readLine();
//        TrackPoint point;
//        do{
//            assert str != null;
//            point = new TrackPoint(str);
//            bw.write(str);
//            bw.newLine();
//            str = br.readLine();
//        } while (point.timestamp < Constants.winStartTime + 1000*60*120 && str != null);
//        bw.close();
//        br.close();


        readInputFile( "D:\\研究生资料\\track_data\\北京出租车\\merge\\Experiment\\convert",
                "D:\\研究生资料\\track_data\\北京出租车\\merge\\Experiment\\Parallel\\", 4);

//        testRedis();

//        testProtoBuff();
    }

    private static void readInputFile(String inputFN, String outputFPN, int outs) throws Exception {
        File inF = new File(inputFN);
        BufferedReader br = new BufferedReader(new FileReader(inF));
        BufferedWriter[] bws = new BufferedWriter[outs];
        for (int j = 0; j < outs; j++) {
            File outF = new File(outputFPN + "part" + j);
            if (!outF.exists()) outF.createNewFile();
            bws[j] = new BufferedWriter(new FileWriter(outF, true));
        }
        String str;
        int index = 0;
        while ((str = br.readLine()) != null){
            bws[index].write(str);
            bws[index].newLine();
            index++;
            index %= outs;
        }
        br.close();
        for (int j = 0; j < outs; j++) {
            bws[j].close();
        }
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
