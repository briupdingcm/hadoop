package com.briup.mr.basic;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Collections;
import java.util.StringTokenizer;

import static com.briup.mr.common.ThrowingConsumer.throwingConsumerWrapper;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1); //统计使用变量
    private Text word = new Text(); //单词变量

    /**
     * key:当前读取行的偏移量
     * value:当前读取的行
     * context:map方法执行时上下文
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        Collections.list(new StringTokenizer(value.toString()))
                .stream().map(t -> ((String) t).trim()).forEach(
                throwingConsumerWrapper(x -> {
                    word.set(x);
                    context.write(word, one);
                }));
    }
}
