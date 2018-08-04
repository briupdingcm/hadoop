package com.briup.mr.join.map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FirstStage {
    public static class SortByKeyMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text k = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split(",");
            k.set(tokens[0]);
            StringBuffer sb = new StringBuffer();
            sb.append(tokens[0]);
            for (int i = 1; i < tokens.length; i++) {
                sb.append(",");
                sb.append(tokens[i]);
            }
            value.set(sb.toString());
            context.write(k, value);

        }
    }

    public static class SortByKeyReducer
            extends Reducer<Text, Text, NullWritable, Text> {
        private static final NullWritable nullKey
                = NullWritable.get();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(nullKey, value);
            }
        }
    }
}
