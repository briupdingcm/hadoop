package com.briup.mr.outputFormat.writer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TarOutputTest extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new TarOutputTest(), args));

    }

    @Override
    public int run(String[] arg0) throws Exception {
        Configuration conf = getConf();
        Path in = new Path(conf.get("input"));
        Path out = new Path(conf.get("output"));

        Job job = Job.getInstance(conf);
        job.setJarByClass(this.getClass());
        job.setJobName("tar");

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TarOutputFormat.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        TextInputFormat.addInputPath(job, in);
        TarOutputFormat.setOutputPath(job, out);
        //TarOutputFormat.setOutputCompressorClass(job, BZip2Codec.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

}
