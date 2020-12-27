package com.ada;

import com.ada.globalTree.GTree;
import com.ada.proto.MyResult;

import java.io.*;
import java.util.*;

public class Test {
    public static void main(String[] args) throws Exception {
        readInputFile();

//        testRedis();

//        testProtoBuff();
    }

    private static void testProtoBuff() throws Exception {
        MyResult.QueryResult.TrackPoint.Builder tpBuilder = MyResult.QueryResult.TrackPoint.newBuilder();
        MyResult.QueryResult.Segment.Builder sBuilder = MyResult.QueryResult.Segment.newBuilder();
        MyResult.QueryResult.Builder qrBuilder = MyResult.QueryResult.newBuilder();

        tpBuilder.setTID(0).setTimeStamp(0).setX(0.0).setY(0.0);
        MyResult.QueryResult.TrackPoint tp0 =  tpBuilder.build();
        tpBuilder.setTID(1).setTimeStamp(1).setX(1.0).setY(1.0);
        MyResult.QueryResult.TrackPoint tp1 =  tpBuilder.build();
        tpBuilder.setTID(2).setTimeStamp(2).setX(2.0).setY(2.0);
        MyResult.QueryResult.TrackPoint tp2 =  tpBuilder.build();
        tpBuilder.setTID(3).setTimeStamp(3).setX(3.0).setY(3.0);
        MyResult.QueryResult.TrackPoint tp3 =  tpBuilder.build();

        sBuilder.setP1(tp0).setP2(tp1);
        MyResult.QueryResult.Segment s0 = sBuilder.build();
        sBuilder.setP1(tp2).setP2(tp3);
        MyResult.QueryResult.Segment s1= sBuilder.build();

        OutputStream os = new FileOutputStream("D:\\研究生资料\\论文\\my paper\\MyPaper\\分布式空间索引\\投递期刊\\Data\\debug\\test");
        BufferedOutputStream bw = new BufferedOutputStream(os);


        ObjectOutputStream oos = new ObjectOutputStream(os);
        qrBuilder.setQueryID(1L).setTimeStamp(1L);
        qrBuilder.addList(s0);
        qrBuilder.addList(s1);
        MyResult.QueryResult qR0 = qrBuilder.build();

        oos.write(qR0.toByteArray());
        qrBuilder.clear();
        qrBuilder.setQueryID(2L).setTimeStamp(2L);
        qrBuilder.addList(s0);
        qrBuilder.addList(s1);
        MyResult.QueryResult qR1 = qrBuilder.build();
        oos.write(qR1.toByteArray());
        os.close();

        InputStream is = new FileInputStream("D:\\研究生资料\\论文\\my paper\\MyPaper\\分布式空间索引\\投递期刊\\Data\\debug\\test");
        ObjectInputStream ois = new ObjectInputStream(is);
        ois.read();
        BufferedInputStream bis = new BufferedInputStream(is);


        MyResult.QueryResult qr2 = MyResult.QueryResult.parseDelimitedFrom(is);
        MyResult.QueryResult qr3 = MyResult.QueryResult.parseDelimitedFrom(is);
        is.close();
        System.out.println(qr2 + " " + qr3);
    }

    private static void readInputFile() throws Exception {
        File inF = new File("D:\\研究生资料\\track_data\\成都滴滴\\Sorted_2D\\XY_20161101");
        File outF = new File("D:\\研究生资料\\track_data\\成都滴滴\\XY_20161101_mn");
        BufferedReader br = new BufferedReader(new FileReader(inF));
        BufferedWriter bw = new BufferedWriter(new FileWriter(outF));
        for (int i = 0; i < 5055517; i++) {
            bw.write(br.readLine());
            bw.newLine();
        }
        br.close();
        bw.close();
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
