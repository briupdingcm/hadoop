package com.briup.rmc.coMatrix;

import com.briup.rmc.common.RecordParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ItemCooccurence extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new ItemCooccurence(), args));
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Path in = new Path(conf.get("input"));
        Path out = new Path(conf.get("output"));

        Job job = Job.getInstance(conf);
        job.setJarByClass(this.getClass());
        job.setJobName("Matrix Preprocess");

        job.setMapperClass(CoocMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setReducerClass(CoocReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setOutputFormatClass(TextOutputFormat.class);

        TextInputFormat.addInputPath(job, in);
        TextOutputFormat.setOutputPath(job, out);
        TextOutputFormat.setCompressOutput(job, true);
        TextOutputFormat.setOutputCompressorClass(job, BZip2Codec.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class CoocMapper extends Mapper<LongWritable, Text, LongWritable, LongWritable> {
        private RecordParser parser = new RecordParser();
        private LongWritable k = new LongWritable();
        private LongWritable v = new LongWritable();

        @Override
        protected void map(LongWritable key, Text value,
                           Context context)
                throws IOException, InterruptedException {
            if (parser.parse(value)) {
                k.set(parser.getUserId());
                v.set(parser.getItemId());
                context.write(k, v);
            }
        }
    }

    public static class CoocReducer extends Reducer<LongWritable, LongWritable, Text, NullWritable> {
        private Text k = new Text();
        private List<Long> items = new ArrayList<Long>();

        @Override
        protected void reduce(LongWritable key, Iterable<LongWritable> values,
                              Context context) throws IOException, InterruptedException {

            items.clear();
            for (LongWritable item : values) {
                items.add(item.get());
            }
            for (int i = 0; i < items.size(); i++) {
                Long k1 = items.get(i);
                for (int j = 0; j < items.size(); j++) {
                    Long k2 = items.get(j);
                    k.set(k1 + "\t" + k2);
                    context.write(k, NullWritable.get());
                }
            }
        }
    }

}
