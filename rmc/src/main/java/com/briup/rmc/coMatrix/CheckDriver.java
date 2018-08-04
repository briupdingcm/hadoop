package com.briup.rmc.coMatrix;

import com.briup.rmc.common.RecordParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

public class CheckDriver extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        // TODO Auto-generated method stub
        System.exit(ToolRunner.run(new CheckDriver(), args));

    }

    @Override
    public int run(String[] arg0) throws Exception {
        Configuration conf = getConf();
        Path input = new Path(conf.get("input"));
        Path vector = new Path(conf.get("vector"));
        Path out = new Path(conf.get("output"));

        Job job = Job.getInstance(conf);
        job.setJarByClass(this.getClass());
        job.setJobName("recommend result");

        MultipleInputs.addInputPath(job, input, TextInputFormat.class, UserPreference.class);
        MultipleInputs.addInputPath(job, vector, TextInputFormat.class, UserRecommend.class);
        job.setMapOutputKeyClass(Key.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, out);

        job.setPartitionerClass(KeyPartitioner.class);
        job.setGroupingComparatorClass(KeyComparator.class);

        job.setReducerClass(UserResult.class);
        job.setOutputKeyClass(Key.class);
        job.setOutputValueClass(DoubleWritable.class);
        TextOutputFormat.setCompressOutput(job, true);
        TextOutputFormat.setOutputCompressorClass(job, BZip2Codec.class);

        return job.waitForCompletion(true) ? 0 : 1;

    }

    public static class Key implements WritableComparable<Key> {
        private Text uiId = new Text();
        private LongWritable tag = new LongWritable();

        public void set(String k, long t) {
            uiId.set(k);
            tag.set(t);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            uiId.readFields(in);
            tag.readFields(in);
        }

        @Override
        public void write(DataOutput out) throws IOException {
            uiId.write(out);
            tag.write(out);
        }

        @Override
        public int compareTo(Key o) {
            return (uiId.compareTo(o.uiId) != 0) ?
                    uiId.compareTo(o.uiId) : tag.compareTo(o.tag);
        }

        @Override
        public String toString() {
            return uiId.toString();
        }

    }

    public static class KeyComparator extends WritableComparator {
        public KeyComparator() {
            super(Key.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            Key k1 = (Key) a;
            Key k2 = (Key) b;
            return k1.uiId.compareTo(k2.uiId);
        }

    }

    public static class KeyPartitioner extends Partitioner<Key, DoubleWritable> {
        @Override
        public int getPartition(Key key, DoubleWritable value, int numPartitions) {
            // TODO Auto-generated method stub
            return Math.abs(key.uiId.hashCode() * 127 % numPartitions);
        }

    }

    public static class UserPreference extends Mapper<LongWritable, Text, Key, DoubleWritable> {
        private RecordParser parser = new RecordParser();
        private Key k = new Key();
        private DoubleWritable v = new DoubleWritable();

        @Override
        protected void map(LongWritable key, Text value,
                           Context context)
                throws IOException, InterruptedException {
            if (parser.parse(value)) {
                k.set(parser.getUserId() + "\t"
                        + parser.getItemId(), 1);
                v.set(parser.getPreference());
                context.write(k, v);
            }
        }
    }

    public static class UserRecommend extends Mapper<LongWritable, Text, Key, DoubleWritable> {
        private Key k = new Key();
        private DoubleWritable v = new DoubleWritable();

        @Override
        protected void map(LongWritable key, Text value,
                           Context context)
                throws IOException, InterruptedException {
            String[] tokens = value.toString().split("\t");
            if (tokens != null && tokens.length == 2) {
                String[] sTokens = tokens[1].trim().split(":");
                k.set(tokens[0].trim() + "\t"
                        + sTokens[0].trim(), 0);
                v.set(Double.parseDouble(sTokens[1].trim()));
                context.write(k, v);
            }
        }
    }

    public static class UserResult extends Reducer<Key, DoubleWritable, Text, DoubleWritable> {
        private DoubleWritable v = new DoubleWritable();
        private Text k = new Text();

        @Override
        protected void reduce(Key key,
                              Iterable<DoubleWritable> values,
                              Context context)
                throws IOException, InterruptedException {
            Iterator<DoubleWritable> iter = values.iterator();
            if (iter.hasNext()) {
                v.set(iter.next().get());
                if (!iter.hasNext()) {
                    k.set(key.toString());
                    context.write(k, v);
                }
            }
        }
    }

}
