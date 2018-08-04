package com.briup.mr.basic;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCountDriver extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new WordCountDriver(), args));
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        Path input = new Path(conf.get("input"));
        Path output = new Path(conf.get("output"));

        Job job = Job.getInstance(conf);        //构建job对象
        job.setJarByClass(WordCountDriver.class);//设置job驱动类
        job.setJobName("Word  Counter");

        job.setMapperClass(WordCountMapper.class);//给job设置mapper类
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(WordCountReducer.class);//给job设置reducer类
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //设置文件的读取方式（文本文件），输出方式（文本文件）
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        //给job指定输入文件的路径和输出结果的路径
        TextInputFormat.addInputPath(job, input);
        TextOutputFormat.setOutputPath(job, output);

        //向集群提交作业
        return job.waitForCompletion(true) ? 0 : 1;
    }
}