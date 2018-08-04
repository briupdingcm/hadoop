package com.briup.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.Arrays;

public class List extends Configured implements Tool {
    private FileSystem fs;

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new List(), args));
    }

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Path path = new Path(conf.get("path"));
        fs = FileSystem.get(conf);
        // 获得文件或目录的元信息对象数组
        FileStatus[] status = fs.listStatus(path);
        Arrays.asList(status).stream().forEach(s -> processStatus(s));
        return 0;
    }

    private void processStatus(FileStatus status) {
        try {
            if (status.isFile() && status.getLen() > 0) out(status);
            else if (status.isDirectory())
                for (FileStatus s : fs.listStatus(status.getPath()))
                    processStatus(s);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void out(FileStatus status) {
        System.out.println("------------------------");
        System.out.println(status.getPath());
        System.out.println(status.getBlockSize());
        System.out.println(status.getPermission());
        System.out.println(status.getReplication());
        System.out.println(status.getOwner());
    }
}