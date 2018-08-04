package com.briup.mr.sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.net.URI;

public class TotalSort extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new TotalSort(), args));
    }

    @Override
    public int run(String[] arg0) throws Exception {
        Configuration conf = getConf();
        Path input = new Path(conf.get("input"));
        Path output = new Path(conf.get("output"));

        Job job = Job.getInstance(conf);
        job.setJarByClass(this.getClass());
        job.setJobName("total sort for weather temp");

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(DoubleWritable.class);
        job.setOutputValueClass(Text.class);

        SequenceFileInputFormat.addInputPath(job, input);
        TextOutputFormat.setOutputPath(job, output);

        job.setPartitionerClass(TotalOrderPartitioner.class);
        /*
         * 构建采样器，从输入的键中采样，分析分布区间
         * (采样因子，最大样本总数，最大分区总数)
         */
        InputSampler.Sampler<DoubleWritable, Text> sampler =
                new InputSampler.RandomSampler<DoubleWritable, Text>(1, 1000, 10);
        /*
         * 运行采样器，计算每个键对应的分布区间，并将计算结果保存
         */
        InputSampler.writePartitionFile(job, sampler);

        // 获得采样器分析的分区结果文件路径名
        String partitionFile = TotalOrderPartitioner.getPartitionFile(conf);
        URI partitionUrl = new URI(partitionFile);
        System.out.println(partitionUrl.toString());
        // 将分区结果分发到集群各节点,Map任务将根据分区文件对数据分区
        job.addCacheFile(partitionUrl);

        return job.waitForCompletion(true) ? 0 : 1;

    }

}
