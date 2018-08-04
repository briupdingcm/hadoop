package com.briup.mr.chain;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class PatentCounter extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new PatentCounter(), args));
    }

    @Override
    public int run(String[] arg0) throws Exception {
        Configuration conf = getConf();
        Path in = new Path(conf.get("input"));
        Path out = new Path(conf.get("output"));
        conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", ",");

        Job job = Job.getInstance(conf, "patent reference count");
        job.setJarByClass(this.getClass());

        ChainMapper.addMapper(job, InverseMapper.class,
                Text.class, Text.class, Text.class, Text.class, conf);
        ChainMapper.addMapper(job, CounterMapper.class,
                Text.class, Text.class, Text.class, IntWritable.class, conf);

        ChainReducer.setReducer(job, IntSumReducer.class,
                Text.class, IntWritable.class, Text.class, IntWritable.class, conf);
        ChainReducer.addMapper(job, InverseMapper.class,
                Text.class, IntWritable.class, IntWritable.class, Text.class, conf);

        job.setInputFormatClass(KeyValueTextInputFormat.class);

        KeyValueTextInputFormat.addInputPath(job, in);
        TextOutputFormat.setOutputPath(job, out);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    static class CounterMapper extends Mapper<Text, Text, Text, IntWritable> {
        private IntWritable one = new IntWritable(1);

        @Override
        protected void map(Text key, Text value, Context context)
                throws IOException, InterruptedException {
            context.write(key, one);
        }
    }
}