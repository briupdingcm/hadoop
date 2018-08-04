package com.briup.mr.outputFormat;

import com.briup.mr.basic.NcdcRecordParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

import static com.briup.mr.common.ThrowingConsumer.throwingConsumerWrapper;

public class PartitionByStation
        extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new PartitionByStation(), args));
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Path in = new Path(conf.get("input"));
        Path out = new Path(conf.get("output"));

        Job job = Job.getInstance(conf);
        job.setJarByClass(this.getClass());
        job.setJobName("partition data");

        job.setMapperClass(StationMapper.class);
        job.setMapOutputKeyClass(Text.class);

        job.setReducerClass(MultipleOutputReducer.class);
        job.setOutputKeyClass(NullWritable.class);

        FileInputFormat.addInputPath(job, in);
        FileOutputFormat.setOutputPath(job, out);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    static class StationMapper extends Mapper<LongWritable, Text, Text, Text> {
        private NcdcRecordParser parser = new NcdcRecordParser();

        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
//			parser.parse(value);
//			if(parser.isValidTemperature()){
//				context.write(new Text(parser.getStationId()), value);
//			}
            parser.parse(() -> value.toString()).ifPresent(throwingConsumerWrapper(p -> {
                //NcdcRecordParser p = (NcdcRecordParser)p1;
                context.write(new Text(p.getStationId()), value);

            }));
        }
    }

    static class MultipleOutputReducer extends Reducer<Text, Text, NullWritable, Text> {
        private MultipleOutputs<NullWritable, Text> multipleOutputs;

        @Override
        protected void setup(Reducer<Text, Text, NullWritable, Text>.Context context) {
            multipleOutputs = new MultipleOutputs<>(context);
        }


        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, NullWritable, Text>.Context context)
                throws IOException, InterruptedException {
//			for(Text v : values){
//				multipleOutputs.write(NullWritable.get(), v, key.toString());
//			}
            values.forEach(throwingConsumerWrapper(v -> {
                multipleOutputs.write(NullWritable.get(), v, key.toString());
            }));
        }

        @Override
        protected void cleanup(Reducer<Text, Text, NullWritable, Text>.Context context)
                throws IOException, InterruptedException {
            multipleOutputs.close();
        }

    }

}
