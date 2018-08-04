package com.briup.mr.type;

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
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.briup.mr.common.ThrowingConsumer.throwingConsumerWrapper;

public class MaxTemperatureByYearStation extends Configured implements Tool {
    public static void main(String... args) throws Exception {
        System.exit(ToolRunner.run(new MaxTemperatureByYearStation(), args));
    }

    @Override
    public int run(String[] arg0) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = getConf();
        Path input = new Path(conf.get("input"));
        Path output = new Path(conf.get("output"));

        Job job = Job.getInstance(conf);
        job.setJarByClass(MaxTemperatureByYearStation.class);
        job.setJobName("max temperature(dingcm)");

        job.setMapperClass(MaxTemperatureMapper.class);
        job.setMapOutputKeyClass(YearStation.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(MaxTemperatureReducer.class);
        job.setOutputKeyClass(YearStation.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        TextInputFormat.addInputPath(job, input);
        TextOutputFormat.setOutputPath(job, output);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    static class MaxTemperatureMapper extends Mapper<LongWritable, Text, YearStation, IntWritable> {
        private YearStation k = new YearStation();
        private IntWritable v = new IntWritable();
        private NcdcRecordParser parser = new NcdcRecordParser();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//			parser.parse(value);
//			if (parser.isValidTemperature()) {
//				k.set(parser.getYear(), parser.getStationId());
//				v.set(parser.getAirTemperature());
//				context.write(k, v);
//			}
            parser.parse(() -> value.toString()).ifPresent(throwingConsumerWrapper(p -> {
                //NcdcRecordParser p = (NcdcRecordParser)p1;
                k.set(p.getYear(), p.getStationId());
                v.set(p.getAirTemperature());
                context.write(k, v);

            }));
        }
    }

    static class MaxTemperatureReducer extends Reducer<YearStation, IntWritable, YearStation, IntWritable> {
        private IntWritable max = new IntWritable();

        public void reduce(YearStation key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            Stream<IntWritable> stream = StreamSupport.stream(values.spliterator(), false);
            int maxValue = stream.map(x -> x.get()).reduce(Integer.MIN_VALUE,
                    (max, curr) -> Math.max(curr, max));

            max.set(maxValue);
            context.write(key, max);
        }
    }

}
