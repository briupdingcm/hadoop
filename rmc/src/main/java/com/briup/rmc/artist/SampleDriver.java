package com.briup.rmc.artist;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Random;

public class SampleDriver extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new SampleDriver(), args));
    }

    @Override
    public int run(String[] arg0) throws Exception {
        Configuration conf = getConf();
        Path in = new Path("/data/audioscrobbler/user_artist_data.txt");
        Path out = new Path("/data/audioscrobbler/artist_split");
        Job job = Job.getInstance(conf);
        job.setJarByClass(this.getClass());
        job.setJobName("artist sample driver");
        job.setReducerClass(SampleReducer.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);

        TextInputFormat.addInputPath(job, in);
        TextOutputFormat.setOutputPath(job, out);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    static class SampleReducer extends Reducer<LongWritable, Text, NullWritable, Text> {
        private MultipleOutputs<NullWritable, Text> multipleOutputs;
        private Random random = new Random();

        @Override
        protected void cleanup(Reducer<LongWritable, Text, NullWritable, Text>.Context context)
                throws IOException, InterruptedException {
            multipleOutputs.close();
        }

        @Override
        protected void setup(Reducer<LongWritable, Text, NullWritable, Text>.Context context) {
            multipleOutputs = new MultipleOutputs<>(context);
            random.nextInt(100);

        }

        @Override
        protected void reduce(LongWritable arg0, Iterable<Text> values,
                              Reducer<LongWritable, Text, NullWritable, Text>.Context context) throws IOException, InterruptedException {
            for (Text v : values) {
                int seed = random.nextInt(100);
                if (seed > 70) {
                    multipleOutputs.write(NullWritable.get(), v, "test");
                } else {
                    multipleOutputs.write(NullWritable.get(), v, "model");

                }
            }
        }

    }

}
