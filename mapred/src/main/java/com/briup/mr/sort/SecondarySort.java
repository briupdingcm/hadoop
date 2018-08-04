package com.briup.mr.sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

import static com.briup.mr.common.ThrowingConsumer.throwingConsumerWrapper;

public class SecondarySort extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new SecondarySort(), args));
    }

    @Override
    public int run(String[] arg0) throws Exception {
        Configuration conf = getConf();
        Path input = new Path(conf.get("input"));
        Path output = new Path(conf.get("output"));
        Job job = Job.getInstance(conf);
        job.setJarByClass(this.getClass());
        job.setJobName("secondary sort");

        job.setMapperClass(SortMapper.class);
        job.setMapOutputKeyClass(YearTemp.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(SortReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        TextInputFormat.addInputPath(job, input);
        TextOutputFormat.setOutputPath(job, output);

        job.setPartitionerClass(YearTempPartitioner.class);
        job.setGroupingComparatorClass(YearTempGroupComparator.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    static class SortMapper extends Mapper<LongWritable, Text, YearTemp, Text> {
        private YearTempParser ytp = new YearTempParser();
        private YearTemp k = new YearTemp();
        private Text v = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            ytp.parse(value);
            if (ytp.isValid()) k.set(ytp.getYear(), ytp.getTemperature());
            v.set(ytp.getStation());
            context.write(k, v);
        }
    }

    static class SortReducer extends Reducer<YearTemp, Text, IntWritable, Text> {
        private IntWritable k = new IntWritable();
        private Text v = new Text();

        @Override
        protected void reduce(YearTemp key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            k.set(key.getYear().get());
//			for (Text station : values) {
//				v.set(station + "\t" + key.getTemperature());
//				context.write(k, v);
//			}
            values.forEach(throwingConsumerWrapper(station -> {
                v.set(station + "\t" + key.getTemperature());
                context.write(k, v);
            }));
        }

    }

}
