package com.briup.rmc.coMatrix;

import com.briup.rmc.common.Heartbeat;
import com.briup.rmc.common.Preference;
import com.briup.rmc.common.VectorParser;
import com.briup.rmc.common.VectorWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
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

public class MatrixVectorMultiple extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new MatrixVectorMultiple(), args));
    }

    @Override
    public int run(String[] arg0) throws Exception {
        Configuration conf = getConf();

        Path user = new Path(conf.get("user"));
        Path item = new Path(conf.get("item"));
        Path out = new Path(conf.get("output"));

        Job job = Job.getInstance(conf);
        job.setJarByClass(this.getClass());
        job.setJobName("recommend list");

        MultipleInputs.addInputPath(job, user, TextInputFormat.class, ItemUserMapper.class);
        MultipleInputs.addInputPath(job, item, TextInputFormat.class, ItemItemMapper.class);
        job.setMapOutputKeyClass(PreferenceOrCooccurence.class);
        job.setMapOutputValueClass(VectorWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, out);
        TextOutputFormat.setCompressOutput(job, true);
        TextOutputFormat.setOutputCompressorClass(job, BZip2Codec.class);
        job.setPartitionerClass(ItemPartitioner.class);
        job.setGroupingComparatorClass(ItemComparator.class);

        job.setReducerClass(MultpleReducer.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Preference.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class PreferenceOrCooccurence implements WritableComparable<PreferenceOrCooccurence> {
        private LongWritable itemId = new LongWritable();
        private LongWritable tag = new LongWritable();

        @Override
        public void readFields(DataInput in) throws IOException {
            itemId.readFields(in);
            tag.readFields(in);
        }

        @Override
        public void write(DataOutput out) throws IOException {
            itemId.write(out);
            tag.write(out);
        }

        @Override
        public int compareTo(PreferenceOrCooccurence o) {
            int cmp = itemId.compareTo(o.itemId);
            if (cmp == 0)
                cmp = tag.compareTo(o.tag);
            return cmp;
        }

        @Override
        public String toString() {
            return itemId + ":" + tag;
        }

        public long getItemId() {
            return itemId.get();
        }

        public void setItemId(long itemId) {
            this.itemId.set(itemId);
        }

        public long getTag() {
            return tag.get();
        }

        public void setTag(long tag) {
            this.tag.set(tag);
        }

    }

    public static class ItemPartitioner extends Partitioner<PreferenceOrCooccurence, VectorWritable> {

        @Override
        public int getPartition(PreferenceOrCooccurence key, VectorWritable value, int numPartitions) {
            return (int) Math.abs(key.getItemId() * 127 % numPartitions);
        }

    }

    public static class ItemComparator extends WritableComparator {
        public ItemComparator() {
            super(PreferenceOrCooccurence.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            PreferenceOrCooccurence p1 = (PreferenceOrCooccurence) a;
            PreferenceOrCooccurence p2 = (PreferenceOrCooccurence) b;
            return p1.itemId.compareTo(p2.itemId);
        }

    }

    public static class ItemUserMapper extends Mapper<LongWritable, Text, PreferenceOrCooccurence, VectorWritable> {
        private PreferenceOrCooccurence k = new PreferenceOrCooccurence();
        private VectorParser parser = new VectorParser();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (parser.parse(value)) {
                k.setItemId(parser.getItemId().get());
                k.setTag(0);
                context.write(k, parser.getUserVec());
                parser.getUserVec().clear();
            }

        }

    }

    public static class ItemItemMapper extends Mapper<LongWritable, Text, PreferenceOrCooccurence, VectorWritable> {
        private PreferenceOrCooccurence k = new PreferenceOrCooccurence();
        private VectorParser parser = new VectorParser();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (parser.parse(value)) {
                k.setItemId(parser.getItemId().get());
                k.setTag(1);
                context.write(k, parser.getUserVec());
                parser.getUserVec().clear();

            }

        }
    }

    public static class MultpleReducer
            extends Reducer<PreferenceOrCooccurence, VectorWritable, LongWritable, Preference> {
        VectorWritable uw = new VectorWritable();
        private LongWritable k = new LongWritable();
        private Heartbeat heartbeat = null;

        @Override
        protected void cleanup(
                Reducer<PreferenceOrCooccurence, VectorWritable, LongWritable, Preference>.Context context) {
            heartbeat.stopbeating();
        }

        @Override
        protected void setup(
                Reducer<PreferenceOrCooccurence, VectorWritable, LongWritable, Preference>.Context context) {
            heartbeat = Heartbeat.createHeartbeat(context);
        }

        @Override
        protected void reduce(PreferenceOrCooccurence pc, Iterable<VectorWritable> values, Context context)
                throws IOException, InterruptedException {
            Iterator<VectorWritable> iter = values.iterator();
            while (iter.hasNext()) {
                VectorWritable iuv = iter.next();
                uw.clear();
                for (Preference pref : iuv)
                    uw.add(new Preference(pref));

                if (iter.hasNext()) {
                    VectorWritable iiv = iter.next();
                    for (Preference p : uw) {
                        k.set(p.getId().get());
                        Iterator<Preference> iterR = iiv.iterator();
                        while (iterR.hasNext()) {
                            Preference rp = iterR.next().multi(p.getValue().get());
                            context.write(k, rp);
                        }
                    }

                }
            }
        }


    }
}
