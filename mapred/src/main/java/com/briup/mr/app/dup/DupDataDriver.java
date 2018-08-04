package com.briup.mr.app.dup;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Collections;
import java.util.StringTokenizer;
import java.util.stream.Collectors;

public class DupDataDriver
        extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        // TODO Auto-generated method stub
        System.exit(ToolRunner.run(new DupDataDriver(), args));
    }

    @Override
    public int run(String[] arg0) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = getConf();
        Path input = new Path(conf.get("input"));
        Path output = new Path(conf.get("output"));

        Job job = Job.getInstance(conf);
        job.setJarByClass(this.getClass());
        job.setJobName("Remove Duplicate Data");

        job.setMapperClass(DupMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setReducerClass(DupReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        TextInputFormat.addInputPath(job, input);
        TextOutputFormat.setOutputPath(job, output);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class DupMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
        private Text pair = new Text();

        @Override
        protected void map(LongWritable key, Text value,
                           Mapper<LongWritable, Text, Text, NullWritable>.Context context)
                throws IOException, InterruptedException {
//			String[] tokens = value.toString().split(",");
//			if(tokens != null && tokens.length == 2){
//				if(!tokens[0].trim().equals(tokens[1].trim())){
//					pair.set(getPair(tokens[0].trim(), tokens[1].trim()));
//					context.write(pair, NullWritable.get());
//				}
//			}
            String res = Collections.list(new StringTokenizer(value.toString(), ","))
                    .stream().map(x -> ((String) x).trim()).sorted().collect(Collectors.joining(","));
            pair.set(res);
            context.write(pair, NullWritable.get());
        }

        private String getPair(String first, String second) {
            if (first.compareTo(second) < 0)
                return first + "," + second;
            return second + "," + first;
        }
    }

    public static class DupReducer extends Reducer<Text, NullWritable, Text, NullWritable> {

        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context)
                throws IOException, InterruptedException {
            context.write(key, NullWritable.get());
        }
    }

}
