package com.ada;

import com.ada.common.Arrays;
import com.ada.common.Constants;
import redis.clients.jedis.Jedis;

import java.io.*;
import java.nio.charset.StandardCharsets;

public class Test {
    public static void main(String[] args) throws Exception {
        testRedis();

//        readInputFile();
    }

    private static void readInputFile() throws IOException {
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

    private static void testRedis() {
        Jedis jedis = new Jedis("localhost");
        jedis.flushDB();
        jedis.flushAll();
        int[][] intArrys1 = new int[512][512];
        setArrayValue(intArrys1);
        int[][] intArrys2 = Arrays.cloneIntMatrix(intArrys1);
        setArrayValue(intArrys2);
        jedis.set("testSegments".getBytes(StandardCharsets.UTF_8), Arrays.toByteArray(intArrys1));
        int[][] myTestR = (int[][]) Arrays.toObject(jedis.get("testSegments".getBytes(StandardCharsets.UTF_8)));
        System.out.println(myTestR);
        jedis.del("testSegments".getBytes(StandardCharsets.UTF_8));
        jedis.close();
    }

    private static void setArrayValue(int[][] intArrays){
        int inc = 0;
        for (int i = 0; i < intArrays.length; i++) {
            for (int j = 0; j < intArrays[i].length; j++) {
                intArrays[i][j] = inc++;
            }
        }
    }
}
