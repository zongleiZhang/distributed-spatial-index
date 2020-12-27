package com.ada.flinkFunction;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.io.File;
import java.io.FileOutputStream;
import java.io.ObjectOutputStream;

public class WriteObjectSF<T> extends RichSinkFunction<T> {
    private String path;
    private String prefix;
    private FileOutputStream fos;
    private ObjectOutputStream oos;
    private int count;

    public WriteObjectSF(){}

    public WriteObjectSF(String path, String prefix) {
        File f = new File(path);
        if (!f.exists()){
            f.mkdir();
        } else {
            for (File file : f.listFiles()) {
                file.delete();
            }
        }
        this.path = path;
        this.prefix = prefix;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        File f = new File(path, prefix + "_" +getRuntimeContext().getIndexOfThisSubtask());
        if (!f.exists()) f.createNewFile();
        fos = new FileOutputStream(f);
        oos = new ObjectOutputStream(fos);
    }

    @Override
    public void close() throws Exception {
        super.close();
        oos.close();
        fos.close();
        System.out.println("WriteObjectSF count:" + count);
    }

    @Override
    public void invoke(T value, Context context) throws Exception {
        oos.writeObject(value);
    }
}
