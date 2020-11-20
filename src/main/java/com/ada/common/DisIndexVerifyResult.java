package com.ada.common;

import com.ada.trackSimilar.Segment;
import com.ada.trackSimilar.TrackPoint;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 分布式索引实验结果的正确性验证
 */
public class DisIndexVerifyResult {


    private static void compareRes() throws Exception {
        Map<Segment, List<Segment>> map0 = new HashMap<>();
        Map<Segment, List<Segment>> map1 = new HashMap<>();
        readRES("F:\\data\\normalRES" + Constants.logicWindow + ".txt", map0);
//		readMutiRES("F:\\data\\normalRES" + Constants.logicWindow, map0);
        readRES("F:\\data\\oneNodeRES" + Constants.logicWindow +".txt", map1);
//		readMutiRES("F:\\data\\normalRES" + Constants.logicWindow, map1);
        map1.forEach((querySeg, resSegs) -> {
            List<Segment> normalRess = map0.get(querySeg);
            if (normalRess!= null) {
                if (resSegs.size() < normalRess.size())
                    System.out.println(1);
                for (Segment resSeg : resSegs) {
                    if (!normalRess.contains(resSeg))
                        System.out.println(1);
                }
            }
        });
        System.out.println(1);
    }

    private static void compareResPoint() throws Exception {
        Map<TrackPoint, List<TrackPoint>> map0 = new HashMap<>();
        Map<TrackPoint, List<TrackPoint>> map1 = new HashMap<>();
        readRESPoint("F:\\data\\normalRES" + Constants.logicWindow + ".txt", map0);
        readRESPoint("F:\\data\\oneNodeRES" + Constants.logicWindow +".txt", map1);

        map1.forEach((querySeg, resSegs) -> {
            List<TrackPoint> normalRess = map0.get(querySeg);
            if (normalRess!= null) {
                if (resSegs.size() < normalRess.size())
                    System.out.println(1);
                for (TrackPoint resSeg : resSegs) {
                    if (!normalRess.contains(resSeg))
                        System.out.println(1);
                }
            }
        });
        System.out.println(1);
    }

    private static void readRESPoint(String fileName, Map<TrackPoint, List<TrackPoint>> resMap) throws Exception {
        File file = new File(fileName);
        BufferedReader br = new BufferedReader(new FileReader(file));
        String line;
        while ( (line = br.readLine()) != null){
            String[] split = line.split("\t");
            TrackPoint querySeg = strToTrackPoint(split[0]);
            List<TrackPoint> res = new ArrayList<>();
            for (int i = 1; i < split.length; i++) {
                TrackPoint segment = strToTrackPoint(split[i]);
                res.add(segment);
            }
            resMap.put(querySeg, res);
        }
    }

    private static void readMutiRES(String pathStr, Map<Segment, List<Segment>> map) throws Exception {
        File path = new File(pathStr);
        File[] files = path.listFiles();
        for (File file : files) {
            BufferedReader br = new BufferedReader(new FileReader(file));
            String line;
            while ( (line = br.readLine()) != null){
                String[] split = line.split("\t");
                Segment querySeg = strToSegment(split[0]);
                List<Segment> res = new ArrayList<>();
                for (int i = 1; i < split.length; i++) {
                    Segment segment = strToSegment(split[i]);
                    res.add(segment);
                }
                List<Segment> list = map.get(querySeg);
                if (list == null){
                    map.put(querySeg, res);
                }else {
                    for (Segment segment : res) {
                        if (!list.contains(segment))
                            list.add(segment);
                    }
                }
            }
        }
    }

    private static void readRES(String fileName, Map<Segment, List<Segment>> resMap) throws Exception {
        File file = new File(fileName);
        BufferedReader br = new BufferedReader(new FileReader(file));
        String line;
        while ( (line = br.readLine()) != null){
            String[] split = line.split("\t");
            Segment querySeg = strToSegment(split[0]);
            List<Segment> res = new ArrayList<>();
            for (int i = 1; i < split.length; i++) {
                Segment segment = strToSegment(split[i]);
                res.add(segment);
            }
            resMap.put(querySeg, res);
        }
    }

    private static Segment strToSegment(String str) {
        String[] split = str.split(" ");
        int TID = Integer.parseInt(split[0]);
        long timeStamp = Long.parseLong(split[1]);
        double[] data1 = new double[]{Double.parseDouble(split[2]), Double.parseDouble(split[3])};
        double[] data2 = new double[]{Double.parseDouble(split[4]), Double.parseDouble(split[5])};
        Segment segment = new Segment();
        segment.p1 = new TrackPoint(data1, timeStamp, TID);
        segment.p2 = new TrackPoint(data2, timeStamp, TID);
        return segment;
    }

    private static TrackPoint strToTrackPoint(String str) {
        String[] split = str.split(" ");
        int TID = Integer.parseInt(split[0]);
        long timeStamp = Long.parseLong(split[1]);
        double[] data = new double[]{Double.parseDouble(split[2]), Double.parseDouble(split[3])};
        return new TrackPoint(data,timeStamp,TID);
    }
}
