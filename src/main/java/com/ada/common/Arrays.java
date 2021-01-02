package com.ada.common;

import java.io.*;

public class Arrays {

    public static int[][] cloneIntMatrix (int[][] a){
        int[][] result = new int[a.length][a[0].length];
        for (int i = 0; i < a.length; i++) {
            result[i] = a[i].clone();
        }
        return result;
    }


    /**
     * 两个同型矩阵a,b。将b中的每个元素加到（isAdd是true）或者去减（isAdd是false）a中的对应元素上。
     */
    public static void addArrsToArrs (int[][] a, int[][] b, boolean isAdd){
        if (isAdd){
            for (int i = 0; i < a.length; i++) {
                for (int j = 0; j < a[i].length; j++)
                    a[i][j] += b[i][j];
            }
        }else {
            for (int i = 0; i < a.length; i++) {
                for (int j = 0; j < a[i].length; j++)
                    a[i][j] -= b[i][j];
            }
        }
    }

    public static boolean arrsEqual (int[][] a, int[][] b){
        if (a == null && b == null) return true;
        if (a == null || b == null) return false;
        if (a.length != b.length) return false;
        for (int i = 0; i < a.length; i++) {
            if (a[i].length != b[i].length) return false;
            for (int j = 0; j < a[i].length; j++) {
                if (a[i][j] != b[i][j]) return false;
            }
        }
        return true;
    }


    /**
     * 对象转数组
     */
    public static byte[] toByteArray (Object obj) {
        byte[] bytes = null;
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try {
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(obj);
            oos.flush();
            bytes = bos.toByteArray ();
            oos.close();
            bos.close();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return bytes;
    }

    /**
     * 数组转对象
     */
    public static Object toObject (byte[] bytes) {
        Object obj = null;
        try {
            ByteArrayInputStream bis = new ByteArrayInputStream (bytes);
            ObjectInputStream ois = new ObjectInputStream (bis);
            obj = ois.readObject();
            ois.close();
            bis.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return obj;
    }
}
