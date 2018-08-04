package com.briup.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import static java.lang.System.out;

public class Cat {

    public static void main(String[] args) {
        Configuration conf = new Configuration();
        FileSystem fs = null;
        try {
            // 获得相对应的fs对象（由uri或conf中的fs.defaultFS决定动态类型）
            fs = FileSystem.get(URI.create(args[0]), conf);
            InputStream is = fs.open(new Path(args[0]));
            // 打开输入流is（动态类型由fs1的动态类型决定)

            byte[] buffer = new byte[4096];
            int len = 0;
            while ((len = is.read(buffer)) > 0)
                out.write(buffer, 0, len);

            out.println("-----------------------");
            out.println(fs.getClass().getName());
            out.println(is.getClass().getName());
            out.println(conf.get("fs.defaultFS"));
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (fs != null)
                    fs.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

}
