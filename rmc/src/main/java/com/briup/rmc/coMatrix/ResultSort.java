package com.briup.rmc.coMatrix;

import com.briup.rmc.common.Preference;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;

public class ResultSort extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new ResultSort(), args));
    }

    @Override
    public int run(String[] arg0) throws Exception {
        Configuration conf = getConf();
        Path input = new Path(conf.get("input"));

        Job job = Job.getInstance(conf);
        job.setJarByClass(this.getClass());
        job.setJobName("result to db");

        TextInputFormat.addInputPath(job, input);

        DBConfiguration.configureDB(
                job.getConfiguration(),
                "com.mysql.jdbc.Driver",
                "jdbc:mysql://172.16.0.100:3306/hadoop",
                "hadoop", "hadoop");
        DBOutputFormat.setOutput(job,
                "recommend_tbl", "userId", "itemId",
                "preference");

        job.setMapperClass(ResultMapper.class);
        job.setMapOutputKeyClass(Preference.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setSortComparatorClass(ValueComparator.class);
        job.setPartitionerClass(ResultPartitioner.class);
        job.setGroupingComparatorClass(ResultComparator.class);

        job.setReducerClass(ResultReducer.class);
        job.setOutputKeyClass(ResultDB.class);
        job.setOutputValueClass(NullWritable.class);

        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(DBOutputFormat.class);

        return job.waitForCompletion(true) ? 0 : 1;

    }

    public static class ResultPartitioner
            extends Partitioner<Preference, LongWritable> {

        @Override
        public int getPartition(Preference key, LongWritable value, int numPartitions) {
            return Math.abs(key.hashCode() * 127 % numPartitions);
        }
    }

    public static class ResultComparator extends WritableComparator {
        public ResultComparator() {
            super(Preference.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            Preference p1 = (Preference) a;
            Preference p2 = (Preference) b;

            return p1.getId().compareTo(p2.getId());
        }

    }

    public static class ValueComparator extends WritableComparator {
        public ValueComparator() {
            super(Preference.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            Preference p1 = (Preference) a;
            Preference p2 = (Preference) b;

            return (p1.getId().compareTo(p2.getId()) != 0) ?
                    p1.getId().compareTo(p2.getId()) : p1.getValue().compareTo(p2.getValue());

        }

    }

    public static class ResultDB implements DBWritable, WritableComparable<ResultDB> {
        private long userId;
        private long itemId;
        private double value;

        public void set(long userId, long itemId, double value) {
            this.userId = userId;
            this.itemId = itemId;
            this.value = value;
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            userId = in.readLong();
            itemId = in.readLong();
            value = in.readDouble();
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeLong(userId);
            out.writeLong(itemId);
            out.writeDouble(value);
        }

        @Override
        public int compareTo(ResultDB o) {
            return (int) ((userId - o.userId) != 0 ? (userId - o.userId) :
                    (itemId - o.itemId));
        }

        @Override
        public void write(PreparedStatement ps) throws SQLException {
            ps.setLong(1, userId);
            ps.setLong(2, itemId);
            ps.setDouble(3, value);
        }

        @Override
        public void readFields(ResultSet resultSet) {
            //写入数据库的对象，不需要实现该方法
        }

    }

    public static class ResultMapper extends Mapper<Text, Text, Preference, LongWritable> {
        private Preference k = new Preference();
        private LongWritable v = new LongWritable();

        @Override
        protected void map(Text key, Text value, Mapper<Text, Text, Preference, LongWritable>.Context context)
                throws IOException, InterruptedException {
            String[] tokens = value.toString().split("\t");
            k.setId(Long.parseLong(key.toString().trim()));
            k.setValue(Double.parseDouble(tokens[1].trim()));
            v.set(Long.parseLong(tokens[0].trim()));
            context.write(k, v);
        }

    }

    public static class ResultReducer extends Reducer<Preference, LongWritable, ResultDB, NullWritable> {
        private ResultDB k = new ResultDB();

        @Override
        protected void reduce(Preference key, Iterable<LongWritable> values,
                              Reducer<Preference, LongWritable, ResultDB, NullWritable>.Context context)
                throws IOException, InterruptedException {
            int num = Integer.parseInt(
                    context.getConfiguration()
                            .get("recommendNum"));
            Iterator<LongWritable> iter = values.iterator();
            int i = 0;
            while (iter.hasNext() && (i < num)) {
                k.set(key.getId().get(), iter.next().get(),
                        key.getValue().get());
                context.write(k, NullWritable.get());
                i++;
            }
        }

    }


}
