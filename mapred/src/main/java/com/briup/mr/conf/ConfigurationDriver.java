package com.briup.mr.conf;

import com.briup.mr.basic.MaxTemperatureDriver;
import com.briup.mr.basic.MaxTemperatureDriver.TemperatureMapper;
import com.briup.mr.basic.MaxTemperatureDriver.TemperatureReducer;
import com.briup.mr.comparator.YearComparator;
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

public class ConfigurationDriver extends Configured implements Tool {


    public static void main(String[] args) throws Exception {
        // TODO Auto-generated method stub
        System.exit(ToolRunner.run(
                new ConfigurationDriver(), args));
    }

    @Override
    public int run(String[] arg0) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = getConf();
        Path input = new Path(conf.get("input"));
        Path output = new Path(conf.get("output"));
        conf.set("mapreduce.job.inputformat.class",
                "org.apache.hadoop.mapreduce.lib.input.TextInputFormat");
        conf.set("mapreduce.job.outputformat.class",
                "org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat");
        Job job = Job.getInstance(conf);
        job.setJarByClass(MaxTemperatureDriver.class);
        job.setJobName("configuration for output");

        job.setMapperClass(TemperatureMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setSortComparatorClass(YearComparator.class);

        job.setReducerClass(TemperatureReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        //	job.setOutputFormatClass(TextOutputFormat.class);

        TextInputFormat.addInputPath(job, input);
        TextOutputFormat.setOutputPath(job, output);

        return job.waitForCompletion(true) ? 0 : 1;

    }
}
