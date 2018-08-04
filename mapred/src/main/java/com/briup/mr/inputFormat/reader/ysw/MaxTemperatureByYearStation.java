package com.briup.mr.inputFormat.reader.ysw;

import com.briup.mr.type.YearStation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class MaxTemperatureByYearStation extends Configured implements Tool {

    public static void main(String... args) throws Exception {
        System.exit(ToolRunner.run(new MaxTemperatureByYearStation(), args));
    }

    @Override
    public int run(String[] arg0) throws Exception {
        Configuration conf = getConf();
        Job job = Job.getInstance(conf);
        job.setJarByClass(MaxTemperatureByYearStation.class);
        job.setJobName("Max Temperature By Year Station");

        job.setInputFormatClass(YearStationInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        YearStationInputFormat.addInputPath(job, new Path(conf.get("input")));
        FileOutputFormat.setOutputPath(job, new Path(conf.get("output")));

        job.setReducerClass(MaxTemperatureReducer.class);
        job.setOutputKeyClass(YearStation.class);
        job.setOutputValueClass(IntWritable.class);

        return (job.waitForCompletion(true) ? 0 : 1);
    }

    static class MaxTemperatureReducer extends Reducer<YearStation, IntWritable, YearStation, IntWritable> {
        public void reduce(YearStation key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int maxValue = Integer.MIN_VALUE;
            for (IntWritable value : values) {
                maxValue = Math.max(maxValue, value.get());
            }
            context.write(key, new IntWritable(maxValue));
        }
    }
}
