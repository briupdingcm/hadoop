package com.briup.mr.join.map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.join.CompositeInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.ArrayList;
import java.util.List;

/**
 * @author kevin
 * map-end join
 * 分成两个阶段的job
 * 第一阶段的结果需满足如下条件：(firstJob,secondJob)
 * 1.参与连接的所有文件的分区数相同
 * 2.参与连接的所有文件按连接键局部排序
 * 3.参与连接的所有文件不能再分片
 * 第二阶段将连接结果输出或进一步处理
 */
public class MapJoinDriver extends Configured implements Tool {
    public static void main(String... args) throws Exception {
        System.exit(ToolRunner.run(new MapJoinDriver(), args));

    }

    @Override
    public int run(String[] args) throws Exception {
        //第一阶段：两个数据集根据连接键局部排序，
        //并且分区数相同
        int numReducers = 2;
        Configuration conf = getConf();
        //第一阶段输入数据集路径
        String jobOneInputPath =
                conf.get("oneInput");
        String jobTwoInputPath =
                conf.get("twoInput");
        //第一阶段输出结果路径
        String jobOneSortedPath = "one_sorted";
        String jobTwoSortedPath = "two_sorted";

        String joinJobOutPath = conf.get("output");

        Job firstJob = Job.getInstance(conf);
        firstJob.setJarByClass(this.getClass());
        firstJob.setJobName("first sort job");
        firstJob.setInputFormatClass(TextInputFormat.class);
        firstJob.setOutputFormatClass(TextOutputFormat.class);
        firstJob.setNumReduceTasks(numReducers);
        firstJob.setMapperClass(FirstStage.SortByKeyMapper.class);
        firstJob.setReducerClass(FirstStage.SortByKeyReducer.class);
        firstJob.setMapOutputKeyClass(Text.class);
        firstJob.setMapOutputValueClass(Text.class);
        firstJob.setOutputKeyClass(NullWritable.class);
        firstJob.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(firstJob,
                new Path(jobOneInputPath));
        FileOutputFormat.setOutputPath(firstJob,
                new Path(jobOneSortedPath));
        //结果文件采用压缩格式存储，使得结果文件不可分片
        FileOutputFormat.setOutputCompressorClass(
                firstJob, GzipCodec.class);


        Job secondJob = Job.getInstance(conf);
        secondJob.setJarByClass(this.getClass());

        secondJob.setJobName("second sort job");
        secondJob.setInputFormatClass(TextInputFormat.class);
        secondJob.setOutputFormatClass(TextOutputFormat.class);
        //与第一个job的reducer分区数一致
        secondJob.setNumReduceTasks(numReducers);
        secondJob.setMapperClass(FirstStage.SortByKeyMapper.class);
        secondJob.setReducerClass(FirstStage.SortByKeyReducer.class);
        secondJob.setMapOutputKeyClass(Text.class);
        secondJob.setMapOutputValueClass(Text.class);
        secondJob.setOutputKeyClass(NullWritable.class);
        secondJob.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(secondJob, new Path(jobTwoInputPath));
        FileOutputFormat.setOutputPath(secondJob, new Path(jobTwoSortedPath));
        //结果文件采用压缩格式存储，使得结果文件不可分片
        FileOutputFormat.setOutputCompressorClass(
                secondJob, GzipCodec.class);

        conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator",
                ",");
        //连接表达式构建（inner或者outer）
        String joinExpression =
                CompositeInputFormat.compose(
                        "inner",
                        KeyValueTextInputFormat.class,
                        jobOneSortedPath,
                        jobTwoSortedPath);
        //连接表达式通过configuration传递给
        //各map节点的CompositeInputFormat实例
        conf.set("mapreduce.join.expr", joinExpression);

        Job joinJob = Job.getInstance(conf);
        joinJob.setJobName("map join job");
        joinJob.setJarByClass(this.getClass());

        FileInputFormat.setInputPaths(joinJob,
                new Path(jobOneSortedPath),
                new Path(jobTwoSortedPath));
        TextOutputFormat.setOutputPath(joinJob,
                new Path(joinJobOutPath));

        joinJob.setMapperClass(CombineValuesMapper.class);
        joinJob.setNumReduceTasks(0);
        joinJob.setOutputKeyClass(NullWritable.class);
        joinJob.setOutputValueClass(Text.class);
        joinJob.setInputFormatClass(
                CompositeInputFormat.class);
        //三个job按顺序提交
        List<Job> jobs = new ArrayList<Job>();
        jobs.add(firstJob);
        jobs.add(secondJob);
        jobs.add(joinJob);
        int exitStatus = 0;
        for (Job job : jobs) {
            boolean jobSuccessful = job.waitForCompletion(true);
            if (!jobSuccessful) {
                System.out.println("Error with job " +
                        job.getJobName() + "  "
                        + job.getStatus().getFailureInfo());
                exitStatus = 1;
                break;
            }
        }
        return exitStatus;
    }
}
