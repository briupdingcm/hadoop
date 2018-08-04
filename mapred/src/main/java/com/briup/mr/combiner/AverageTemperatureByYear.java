package com.briup.mr.combiner;

import com.briup.mr.basic.NcdcRecordParser;
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
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.briup.mr.common.ThrowingConsumer.throwingConsumerWrapper;

public class AverageTemperatureByYear extends Configured implements Tool {
    public static void main(String... args) throws Exception {
        System.exit(ToolRunner.run(new AverageTemperatureByYear(), args));
    }

    @Override
    public int run(String[] arg0) throws Exception {
        Configuration conf = getConf();
        Job job = Job.getInstance(conf);
        job.setJarByClass(AverageTemperatureByYear.class);
        job.setJobName("Average Temperature By Year");

        FileInputFormat.addInputPath(job, new Path(conf.get("input")));
        FileOutputFormat.setOutputPath(job, new Path(conf.get("output")));

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapperClass(AverageTemperatureMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(AverageValue.class);

        job.setCombinerClass(AverageTemperatureCombiner.class);

        job.setReducerClass(AverageTemperatureByYear.AverageTemperatureReducer.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(DoubleWritable.class);

        return (job.waitForCompletion(true) ? 0 : 1);
    }

    static class AverageTemperatureMapper extends Mapper<LongWritable, Text, LongWritable, AverageValue> {
        private NcdcRecordParser parser = new NcdcRecordParser();
        private LongWritable year = new LongWritable();
        private AverageValue av = new AverageValue();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			/*parser.parse(value);
			if (parser.isValidTemperature()) {
				year.set(parser.getYear());
				av.setNum(1);
				av.setValue(parser.getAirTemperature());
				context.write(year, av);
			*/
            parser.parse(() -> value.toString()).ifPresent(throwingConsumerWrapper(nrp -> {
                //NcdcRecordParser nrp = (NcdcRecordParser)p;
                year.set(nrp.getYear());
                av.setNum(1);
                av.setValue(nrp.getAirTemperature());
                context.write(year, av);
            }));
        }
    }

    static class AverageTemperatureCombiner extends Reducer<LongWritable, AverageValue, LongWritable, AverageValue> {
        public void reduce(LongWritable key, Iterable<AverageValue> values, Context context)
                throws IOException, InterruptedException {
            Stream<AverageValue> stream = StreamSupport.stream(values.spliterator(), false);
            AverageValue res = stream.reduce(AverageValue.empty(), AverageValue::add);
//			int c = stream.collect(summingInt(x -> x.getNum().get()));
//			double s = stream.collect(summingDouble(x -> x.sum()));
            context.write(key, res);
        }
    }

    static class AverageTemperatureReducer
            extends Reducer<LongWritable, AverageValue, LongWritable, DoubleWritable> {
        public void reduce(LongWritable key, Iterable<AverageValue> values, Context context)
                throws IOException, InterruptedException {
            Stream<AverageValue> stream = StreamSupport.stream(values.spliterator(), false);
            AverageValue res = stream.reduce(AverageValue.empty(), AverageValue::add);

//			int c = stream.collect(summingInt(x -> x.getNum().get()));
//			double s = stream.collect(summingDouble(x -> x.sum()));

            context.write(key, res.getValue());
        }
    }
}
