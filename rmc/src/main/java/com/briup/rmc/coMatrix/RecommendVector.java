package com.briup.rmc.coMatrix;

import com.briup.rmc.common.Preference;
import com.briup.rmc.common.PreferenceParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class RecommendVector extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new RecommendVector(), args));

    }

    @Override
    public int run(String[] arg0) throws Exception {
        Configuration conf = getConf();

        Path in = new Path(conf.get("input"));
        Path out = new Path(conf.get("output"));

        Job job = Job.getInstance(conf);
        job.setJarByClass(this.getClass());
        job.setJobName("recommend vector");

        job.setMapperClass(SumMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Preference.class);

        job.setReducerClass(SumReducer.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Preference.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        SequenceFileInputFormat.addInputPath(job, in);
        TextOutputFormat.setOutputPath(job, out);
        TextOutputFormat.setCompressOutput(job, true);
        TextOutputFormat.setOutputCompressorClass(job, BZip2Codec.class);

        return job.waitForCompletion(true) ? 0 : 1;

    }

    public static class SumMapper extends Mapper<LongWritable, Text, Text, Preference> {
        private PreferenceParser parser = new PreferenceParser();
        private Text k = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            if (parser.parse(value)) {
                k.set(parser.getItemId() + "," + parser.getPreference().getId());
                context.write(k, parser.getPreference());
            }
        }
    }

    public static class SumReducer extends Reducer<Text, Preference,
            LongWritable, Preference> {
        private LongWritable k = new LongWritable();
        private Preference v = new Preference();

        @Override
        protected void reduce(Text key, Iterable<Preference> values, Context context)
                throws IOException, InterruptedException {
            String[] tokens = key.toString().split(",");
            k.set(Long.parseLong(tokens[0].trim()));
            double sum = 0;
            for (Preference p : values) {
                sum += p.getValue().get();
            }
            v.set(Long.parseLong(tokens[1].trim()), sum);
            context.write(k, v);
        }
    }


}
