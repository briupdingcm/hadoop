package com.briup.io.integrity;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;

public class CheckSumReaderTest extends Configured implements Tool {
    private FileSystem fs;

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new CheckSumReaderTest(), args));
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        fs = new RawLocalFileSystem();
        fs.initialize(URI.create(args[0]), conf);
        BufferedReader ps = new BufferedReader(
                new InputStreamReader(fs.open(new Path(args[0]))));
        System.out.println(ps.readLine());

        fs = new LocalFileSystem(fs);
        fs.initialize(URI.create(args[1]), conf);
        ps = new BufferedReader(
                new InputStreamReader(fs.open(new Path(args[1]))));
        System.out.println(ps.readLine());

        return 0;
    }

}
