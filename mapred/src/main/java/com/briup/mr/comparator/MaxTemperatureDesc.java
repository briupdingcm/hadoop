package com.briup.mr.comparator;

import com.briup.mr.basic.NcdcRecordParser;
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
import java.util.Comparator;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.briup.mr.common.ThrowingConsumer.throwingConsumerWrapper;
import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.maxBy;

public class MaxTemperatureDesc
        extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        // TODO Auto-generated method stub
        System.exit(ToolRunner.run(
                new MaxTemperatureDesc(), args));
    }

    @Override
    public int run(String[] arg0) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = getConf();
        Path input = new Path(conf.get("input"));
        Path output = new Path(conf.get("output"));

        Job job = Job.getInstance(conf);
        job.setJarByClass(MaxTemperatureDesc.class);
        job.setJobName("max temperature(dingcm)");

        job.setMapperClass(TemperatureMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);

        //设置键的比较器
        job.setSortComparatorClass(YearComparator.class);

        job.setReducerClass(TemperatureReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        TextInputFormat.addInputPath(job, input);
        TextOutputFormat.setOutputPath(job, output);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    static class TemperatureMapper
            extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
        private IntWritable k = new IntWritable();
        private IntWritable v = new IntWritable();
        private NcdcRecordParser parser = new NcdcRecordParser();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
//			parser.parse(value);
//			if(parser.isValidTemperature()){
//				k.set(parser.getYear());
//				v.set(parser.getAirTemperature());
//				context.write(k, v);
//			}
            parser.parse(() -> value.toString()).ifPresent(throwingConsumerWrapper(p -> {
                //NcdcRecordParser p = (NcdcRecordParser)p1;
                k.set(p.getYear());
                v.set(p.getAirTemperature());
                context.write(k, v);

            }));
        }

    }

    static class TemperatureReducer
            extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        private IntWritable val = new IntWritable();

        @Override
        protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
//			int max = Integer.MIN_VALUE;
//			for(IntWritable v : values){
//				if(v.get() > max) max = v.get();
//			}
//			val.set(max);

            //context.write(key, val);
            Stream<IntWritable> stream = StreamSupport.stream(values.spliterator(), false);

            stream.collect(maxBy(Comparator.naturalOrder())).ifPresent(throwingConsumerWrapper(m -> {
                context.write(key, m);
            }));
        }

    }

}
