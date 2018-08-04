package com.briup.mr.basic;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * 通过继承org.apache.hadoop.mapreduce.Reducer编写自己的Reducer
 */

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    /**
     * key:待统计的word
     * values:待统计word的所有统计标识
     * context:reduce方法执行时的上下文
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context
            context) throws IOException, InterruptedException {
        Stream<IntWritable> stream = StreamSupport.stream(values.spliterator(), false);
        //对values中所有元素求和
        int count = stream.mapToInt(x -> x.get()).sum();

        context.write(key, new IntWritable(count));
    }
}


