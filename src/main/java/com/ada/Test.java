package com.ada;

import com.ada.common.Constants;
import redis.clients.jedis.Jedis;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.nio.charset.StandardCharsets;

public class Test {
    public static void main(String[] args) throws Exception {
//        testRedis();

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
        int[][] intArrys1 = new int[512][512];
        intArrys1[0][1] = 590;
        int[][] intArrys2 = new int[512][512];
        intArrys2[0][1] = 590;
        Jedis jedis = new Jedis("localhost");
        jedis.set("testSegments".getBytes(StandardCharsets.UTF_8), Constants.toByteArray(intArrys1));
        int[][] myTestR = (int[][]) Constants.toObject(jedis.get("testSegments".getBytes(StandardCharsets.UTF_8)));
        System.out.println(myTestR);
        jedis.del("testSegments".getBytes(StandardCharsets.UTF_8));
        jedis.close();
    }
}
