package com.briup.mr.basic;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MinimalMapReduce extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new MinimalMapReduce(), args));
    }

    @Override
    public int run(String[] strings) throws Exception {
        //构建新的作业
        Configuration conf = getConf();
        Path input = new Path(conf.get("input"));
        Path output = new Path(conf.get("output"));

        Job job = Job.getInstance(conf, "MinimalMapReduce");
        job.setJarByClass(MinimalMapReduce.class);
        //设置输入输出路径
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);
        //向集群提交作业
        return job.waitForCompletion(true) ? 0 : 1;
    }
}