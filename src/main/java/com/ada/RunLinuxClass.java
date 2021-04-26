package com.ada;

import java.io.*;
import java.util.Properties;

public class RunLinuxClass {
    private static Runtime run = Runtime.getRuntime();

    public static void main(String[] args) throws Exception {
        Properties prop = new Properties();
        FileInputStream in = new FileInputStream("origin.properties");
        prop.load(in);
        in.close();
        FileOutputStream oFile = new FileOutputStream("conf.properties");
        prop.setProperty("frame", "DIP");
        prop.store(oFile, "The New properties file");
        oFile.close();

        System.out.println(runCommand("F:/softwares/flink-1.9.0/bin/flink.bat run D:/code/java/distributed-spatial-index/target/distributedSpatialIndex-1.0-SNAPSHOT.jar"));
        System.out.println("\n");
        Thread.sleep(1000L*20);
        System.out.println(runCommand("F:\\softwares\\flink-1.9.0\\bin\\flink.bat list"));
    }

    private static String runCommand(String cmd){
        StringBuilder result = new StringBuilder();
        try {
            Process process = run.exec(cmd);
            InputStream in = process.getInputStream();
            BufferedReader bs = new BufferedReader(new InputStreamReader(in));
            String str;
            result.append(bs.readLine());
            while ((str = bs.readLine()) != null) {
                result.append("\n").append(str);
            }
            in.close();
            process.destroy();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result.toString();
    }
}
