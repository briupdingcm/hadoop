package com.briup.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.InputStream;
import java.net.URI;

import static java.lang.System.out;

public class HadoopCat extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new HadoopCat(), args));
    }

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        // 获得相对应的fs对象（由uri或conf中的fs.defaultFS决定动态类型）
        FileSystem fs = FileSystem.get(URI.create(conf.get("input")), conf);
        // 打开输入流is（动态类型由fs的动态类型决定)
        InputStream is = fs.open(new Path(conf.get("input")));
        // 操作流对象
        IOUtils.copyBytes(is, out, 4096, true);
        is.close();
        return 0;
    }
}
