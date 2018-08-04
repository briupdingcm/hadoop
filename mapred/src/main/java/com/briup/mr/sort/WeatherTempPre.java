package com.briup.mr.sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

import static com.briup.mr.common.ThrowingConsumer.throwingConsumerWrapper;

public class WeatherTempPre extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new WeatherTempPre(), args));
    }

    @Override
    public int run(String[] arg0) throws Exception {
        Configuration conf = getConf();
        Path input = new Path(conf.get("input"));
        Path output = new Path(conf.get("output"));

        Job job = Job.getInstance(conf);
        job.setJarByClass(this.getClass());
        job.setJobName("preprocess the weather counter");

        job.setMapperClass(WeatherMapper.class);
        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(WeatherReducer.class);
        job.setOutputKeyClass(DoubleWritable.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        TextInputFormat.addInputPath(job, input);
        SequenceFileOutputFormat.setOutputPath(job, output);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    static class WeatherMapper extends Mapper<LongWritable, Text, DoubleWritable, Text> {
        private DoubleWritable k = new DoubleWritable();

        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, DoubleWritable, Text>.Context context)
                throws IOException, InterruptedException {
            double temperature = Double.parseDouble(value.toString().split("\t")[2]);
            k.set(temperature);
            context.write(k, value);
        }

    }

    static class WeatherReducer extends Reducer<DoubleWritable, Text, DoubleWritable, Text> {
        @Override
        protected void reduce(DoubleWritable key, Iterable<Text> values,
                              Reducer<DoubleWritable, Text, DoubleWritable, Text>.Context context)
                throws IOException, InterruptedException {
            values.forEach(throwingConsumerWrapper(v -> context.write(key, v)));
        }
    }

}
