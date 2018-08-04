package com.briup.io.integrity;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.PrintStream;
import java.net.URI;

public class CheckSumWriterTest extends Configured implements Tool {
    private FileSystem fs;

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new CheckSumWriterTest(), args));
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        fs = new RawLocalFileSystem();
        fs.initialize(URI.create(args[0]), conf);
        PrintStream ps = new PrintStream(fs.create(new Path(args[0])));
        ps.println("hhhhhhhhhhhhhhhhhhhhhhhhhhhhggggggggggggggggggggggggggggggggggggghhhh");
        ps.close();

        fs = new LocalFileSystem(fs);
        fs.initialize(URI.create(args[1]), conf);
        ps = new PrintStream(fs.create(new Path(args[1])));
        ps.println("hhhhhhhhhhhhhhhhhgggggggggggggggggggggggggggggggggggggggghhhhhhhhhhhhhhh");
        ps.close();
        return 0;
    }
}
