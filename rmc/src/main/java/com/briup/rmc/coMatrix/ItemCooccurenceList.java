package com.briup.rmc.coMatrix;

import com.briup.rmc.common.Preference;
import com.briup.rmc.common.VectorWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class ItemCooccurenceList extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new ItemCooccurenceList(), args));
    }

    @Override
    public int run(String[] arg0) throws Exception {
        Configuration conf = getConf();
        Path in = new Path(conf.get("input"));
        Path out = new Path(conf.get("output"));

        Job job = Job.getInstance(conf);
        job.setJarByClass(this.getClass());
        job.setJobName("Matrix List");

        job.setMapperClass(ListMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Preference.class);

        job.setReducerClass(ListReducer.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(VectorWritable.class);

        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        KeyValueTextInputFormat.addInputPath(job, in);
        TextOutputFormat.setOutputPath(job, out);
        TextOutputFormat.setCompressOutput(job, true);
        TextOutputFormat.setOutputCompressorClass(job, BZip2Codec.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class ListMapper
            extends Mapper<Text, Text,
            LongWritable, Preference> {
        private LongWritable k = new LongWritable();
        private Preference v = new Preference();

        @Override
        protected void map(Text key, Text value,
                           Context context) throws IOException, InterruptedException {
            k.set(Long.parseLong(key.toString().trim()));
            String[] tokens = value.toString().split("\t");
            v.set(Long.parseLong(tokens[0].trim()), Double.parseDouble(tokens[1].trim()));
            context.write(k, v);

        }
    }

    public static class ListReducer extends Reducer<LongWritable, Preference, LongWritable, VectorWritable> {
        private VectorWritable v = new VectorWritable();

        @Override
        protected void reduce(LongWritable key,
                              Iterable<Preference> values,
                              Context context)
                throws IOException, InterruptedException {
            v.clear();
            for (Preference p : values) {
                v.add(new Preference(p));
            }
            context.write(key, v);
        }

    }

}
