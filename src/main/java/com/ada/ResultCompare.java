package com.ada;

import com.ada.geometry.Segment;
import com.ada.model.common.result.QueryResult;

import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.util.*;

public class ResultCompare {
    public static void main(String[] args) throws Exception {
        Map<Long, List<Segment>> singleNodeMap = new HashMap<>();
        Map<Long, List<Segment>> multipleNodeMap = new HashMap<>();
        readFile( "D:\\研究生资料\\论文\\my paper\\MyPaper\\分布式空间索引\\投递期刊\\Data\\debug\\SSI_QBS\\output_0", singleNodeMap);
        for (int i = 0; i < 4; i++) {
            readFile( "D:\\研究生资料\\论文\\my paper\\MyPaper\\分布式空间索引\\投递期刊\\Data\\debug\\DSI\\output_" + i, multipleNodeMap);
        }
        for (Map.Entry<Long, List<Segment>> entry : multipleNodeMap.entrySet()) {
            if (singleNodeMap.get(entry.getKey()) == null) {
                System.out.println();
            }else {
                Set<Segment> singleResult = new HashSet<>(singleNodeMap.get(entry.getKey()));
                Set<Segment> multipleResult = new HashSet<>(entry.getValue());
                singleResult.removeAll(entry.getValue());
                multipleResult.removeAll(singleNodeMap.get(entry.getKey()));
                if (!singleResult.isEmpty() || !multipleResult.isEmpty())
                    System.out.print("");
            }
        }
    }

    private static void readFile(String path, Map<Long, List<Segment>> map) throws Exception {
        ObjectInputStream ois = new	ObjectInputStream(new FileInputStream(path));
        QueryResult result = (QueryResult) ois.readObject();
        while (result != null){
            map.put(result.getQueryID(), result.list);
            result = (QueryResult) ois.readObject();
        }
        ois.close();
    }
}
