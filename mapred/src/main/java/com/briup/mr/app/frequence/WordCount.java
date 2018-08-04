package com.briup.mr.app.frequence;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Collections;
import java.util.StringTokenizer;
import java.util.stream.StreamSupport;

import static com.briup.mr.common.ThrowingConsumer.throwingConsumerWrapper;

public class WordCount extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        int status = ToolRunner.run(new WordCount(), args);
        System.exit(status);
    }

    @Override
    public int run(String[] args) throws Exception {
        //获得程序运行时的配置信息
        Configuration conf = getConf();
        String inputPath = conf.get("input");
        String outputPath = conf.get("output");

        //构建新的作业
        Job job = Job.getInstance(conf, "Word Frequence Count");
        job.setJarByClass(WordCount.class);

        //给job设置mapper类及map方法输出的键值类型
        job.setMapperClass(WordCountMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //给job设置reducer类及reduce方法输出的键值类型
        job.setReducerClass(WordCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //设置数据的读取方式（文本文件）及结果的输出方式（文本文件）
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        //设置输入和输出目录
        TextInputFormat.addInputPath(job, new Path(inputPath));
        TextOutputFormat.setOutputPath(job, new Path(outputPath));


        //将作业提交集群执行
        return job.waitForCompletion(true) ? 0 : 1;
    }

    static class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        //		统计使用变量
        private final static IntWritable one = new IntWritable(1);
        //		单词变量
        private Text word = new Text();

        /**
         * key:当前读取行的偏移量
         * value：当前读取的行
         * context:map方法执行时上下文
         */
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub
//			StringTokenizer words=
//					new StringTokenizer(value.toString(), " ");
//
//			while(words.hasMoreTokens()){
//				word.set(words.nextToken());
//				context.write(word, one);
//			}
            Collections.list(new StringTokenizer(value.toString(), ","))
                    .stream().map(x -> ((String) x).trim()).forEach(throwingConsumerWrapper(w -> {
                word.set(w);
                context.write(word, one);
            }));
        }
    }

    static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable counter = new IntWritable();

        /**
         * key:待统计的word
         * values:待统计word的所有统计标识
         * context:reduce方法执行时的上下文
         */
        @Override
        protected void reduce(Text key,
                              Iterable<IntWritable> values,
                              Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            // TODO Auto-generated method stub
//			int count=0;
//			for(IntWritable one:values){
//				count+=one.get();
//			}
//			counter.set(count);
//			context.write(key, counter);
            int count = StreamSupport.stream(values.spliterator(), false).mapToInt(x -> x.get())
                    .reduce(0, (c, e) -> c + e);
            counter.set(count);
            context.write(key, counter);

        }
    }

}
