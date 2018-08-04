package com.briup.mr.inputFormat.reader.xml;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class PersonInfoDriver extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new PersonInfoDriver(), args));

    }

    @Override
    public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = getConf();
        Path input = new Path(conf.get("input"));
        Path output = new Path(conf.get("output"));

        //构建job对象，并设置驱动类名和job名
        Job job = Job.getInstance(conf);
        job.setJarByClass(PersonInfoDriver.class);//设置job驱动类
        job.setJobName("Person Counter");

        //给job设置mapper类及map方法输出的键值类型
        job.setMapperClass(InfoMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setCombinerClass(IntSumReducer.class);

        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //设置文件的读取方式（XML文本文件），输出方式（文本文件）
        job.setInputFormatClass(XMLInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        //给job指定输入文件的路径和输出结果的路径
        XMLInputFormat.addInputPath(job, input);
        TextOutputFormat.setOutputPath(job, output);

        XMLInputFormat.setElementName(job, "student");
        return (job.waitForCompletion(true) ? 0 : 1);
    }

    public static class InfoMapper
            extends Mapper<Text, Text, Text, IntWritable> {
        private IntWritable v = new IntWritable(1);

        @Override
        protected void map(Text key, Text value, Mapper<Text, Text, Text, IntWritable>.Context context)
                throws IOException, InterruptedException {
            context.write(key, v);
        }

    }

}
