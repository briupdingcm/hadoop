package com.briup.mr.basic;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
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
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.briup.mr.common.ThrowingConsumer.throwingConsumerWrapper;

public class PatentReferenceList extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        PatentReferenceList prc = new PatentReferenceList();
        System.exit(ToolRunner.run(prc, args));
    }

    @Override
    public int run(String[] arg0) throws Exception {
        Configuration conf = getConf();
        Path input = new Path(conf.get("input"));
        Path output = new Path(conf.get("output"));

        // 构建job对象，并设置驱动类名和job名
        Job job = Job.getInstance(conf);
        job.setJarByClass(PatentReferenceList.class);// 设置job驱动类
        job.setJobName("Patent Reference Counter");// 设置job名字

        // 给job设置mapper类及map方法产生的键值类型
        job.setMapperClass(PatentMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // 给job设置reducer类及reduce方法产生的键值类型
        job.setReducerClass(PatentReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(5); // mapreduce.job.reduces

        // 设置文件的读取方式（文本文件），输出方式（文本文件）
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // 给job指定输入文件的路径和输出结果的路径
        TextInputFormat.addInputPath(job, input);
        TextOutputFormat.setOutputPath(job, output);

        // 向集群提交作业
        return job.waitForCompletion(true) ? 0 : 1;
    }

    static class PatentMapper extends Mapper<LongWritable, Text, Text, Text> {
        private PatentRecordParser parser = new PatentRecordParser();
        private Text k = new Text();
        private Text v = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // 分割读入的一行
//			parser.parse(value);
//			if (parser.isValid()) {
//				// 构建键值对象
//				k.set(parser.getPatentId());
//				v.set(parser.getRefPatentId());
//				// 传递给MR框架
//				context.write(k, v);
//			}
            parser.parse(() -> value.toString()).ifPresent(throwingConsumerWrapper(prp -> {
                k.set(parser.getPatentId());
                v.set(parser.getRefPatentId());
                context.write(k, v);
            }));
        }
    }

    static class PatentReducer extends Reducer<Text, Text, Text, Text> {
        private Text valu = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Stream<String> stream = StreamSupport.stream(values.spliterator(), false).map(x -> x.toString());
            String res = stream.collect(Collectors.joining(",", "[", "]"));
            // 由map递交给MR框架的数据按照键分组后传递到reduce任务，values即为分组结果，key为这一组的键
            if (res.length() > 1) {
                valu.set(res);
                context.write(key, valu);
            }
        }
    }

}
