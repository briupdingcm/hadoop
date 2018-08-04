package com.briup.mr.type;

import com.briup.mr.basic.NcdcRecordParser;
import com.briup.mr.combiner.AverageValue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Date;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.briup.mr.common.ThrowingConsumer.throwingConsumerWrapper;

public class AVTByYear extends Configured implements Tool {
    public static void main(String... args) throws Exception {
        long start = new Date().getTime();
        ToolRunner.run(new AVTByYear(), args);
        long end = new Date().getTime();
        System.out.println("time: " + (end - start));
        System.exit(1);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Job job = Job.getInstance(conf);
        job.setJarByClass(AVTByYear.class);
        job.setJobName("Average Temperature By Year and Station");

        job.setNumReduceTasks(20);

        FileInputFormat.addInputPath(job, new Path(conf.get("input")));
        FileOutputFormat.setOutputPath(job, new Path(conf.get("output")));

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapperClass(AverageTemperatureMapper.class);
        job.setMapOutputKeyClass(YearStation.class);
        job.setMapOutputValueClass(AverageValue.class);

        job.setCombinerClass(AverageTemperatureCombiner.class);

        job.setReducerClass(AverageTemperatureReducer.class);
        job.setOutputKeyClass(YearStation.class);
        job.setOutputValueClass(DoubleWritable.class);

        return (job.waitForCompletion(true) ? 0 : 1);
    }

    static class AverageTemperatureMapper extends Mapper<LongWritable, Text, YearStation, AverageValue> {
        private NcdcRecordParser parser = new NcdcRecordParser();
        private YearStation k = new YearStation();
        private AverageValue v = new AverageValue();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//			parser.parse(value);
//			if (parser.isValidTemperature()) {
//				k.set(parser.getYear(), parser.getStationId());
//				v.set(1, parser.getAirTemperature());
//				context.write(k, v);
//			}
            parser.parse(() -> value.toString()).ifPresent(throwingConsumerWrapper(p -> {
                //	NcdcRecordParser p = (NcdcRecordParser)p1;
                k.set(p.getYear(), p.getStationId());
                v.set(1, p.getAirTemperature());
                context.write(k, v);

            }));
        }
    }

    static class AverageTemperatureCombiner extends Reducer<YearStation, AverageValue, YearStation, AverageValue> {
        private AverageValue val = new AverageValue();

        public void reduce(YearStation key, Iterable<AverageValue> values, Context context)
                throws IOException, InterruptedException {
            Stream<AverageValue> stream = StreamSupport.stream(values.spliterator(), false);
            AverageValue v = stream.reduce(AverageValue.empty(), AverageValue::add);
            context.write(key, v);
        }
    }

    static class AverageTemperatureReducer
            extends Reducer<YearStation, AverageValue, YearStation, DoubleWritable> {
        private DoubleWritable val = new DoubleWritable();

        public void reduce(YearStation key, Iterable<AverageValue> values, Context context)
                throws IOException, InterruptedException {
            Stream<AverageValue> stream = StreamSupport.stream(values.spliterator(), false);
            AverageValue v = stream.collect(Collectors.reducing(AverageValue.empty(), AverageValue::add));

            context.write(key, v.getValue());
        }
    }

}
