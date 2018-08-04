package com.briup.mr.join.map;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.join.TupleWritable;

import java.io.IOException;

public class CombineValuesMapper extends Mapper<Text, TupleWritable, NullWritable, Text> {

    private static final NullWritable k = NullWritable.get();
    private Text v = new Text();
    private StringBuffer sb = new StringBuffer();
    private String separator = ",";

    @Override
    protected void map(Text key, TupleWritable value, Context context) throws IOException, InterruptedException {
        sb.append(key).append(separator);
        for (Writable writable : value) {
            sb.append(writable.toString()).append(separator);
        }
        sb.setLength(sb.length() - 1);
        v.set(sb.toString());
        context.write(k, v);
        sb.setLength(0);
    }

}
