package com.briup.mr.app.cooccurence;

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
import java.util.stream.StreamSupport;

import static com.briup.mr.common.ThrowingConsumer.throwingConsumerWrapper;
import static java.util.stream.Collectors.joining;

public class FriendList
        extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        // TODO Auto-generated method stub
        System.exit(ToolRunner.run(new FriendList(), args));
    }

    @Override
    public int run(String[] arg0) throws Exception {
        Configuration conf = getConf();
        Path input = new Path(conf.get("input"));
        Path output = new Path(conf.get("output"));

        //构建job对象，并设置驱动类名和job名
        Job job = Job.getInstance(conf);
        job.setJarByClass(FriendList.class);//设置job驱动类
        job.setJobName("Friend List");//设置job名字

        //给job设置mapper类及map方法产生的键值类型
        job.setMapperClass(FriendMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        //给job设置reducer类及reduce方法产生的键值类型
        job.setReducerClass(FriendReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //设置文件的读取方式（文本文件），输出方式（文本文件）
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        //给job指定输入文件的路径和输出结果的路径
        TextInputFormat.addInputPath(job, input);
        TextOutputFormat.setOutputPath(job, output);

        //向集群提交作业
        return job.waitForCompletion(true) ? 0 : 1;
    }

    static class FriendMapper extends Mapper<LongWritable, Text, Text, Text> {
        private FriendRecordParser parser = new FriendRecordParser();
        private Text k = new Text();
        private Text v = new Text();

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            //分割读入的一行
//			parser.parse(value);
//			if(parser.isValid()){
//				//构建键值对象
//				k.set(parser.getUserName());
//				v.set(parser.getFriendName());
//				//传递给MR框架
//				context.write(k, v);
//			}
            parser.parse(() -> value.toString()).ifPresent(throwingConsumerWrapper(p -> {
                //	FriendRecordParser p = (FriendRecordParser)p1;
                k.set(p.getUserName());
                v.set(p.getFriendName());
                //传递给MR框架
                context.write(k, v);
            }));
        }
    }

    static class FriendReducer extends Reducer<Text, Text, Text, Text> {
        private Text val = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //由map递交给MR框架的数据按照键分组后传递到reduce任务，values即为分组结果，key为这一组的键
//			StringBuffer sb = new StringBuffer();
//
//			for(Text v : values){
//				sb.append("," + v.toString());
//			}
//			val.set(sb.toString());
//			context.write(key, val);
            String res = StreamSupport.stream(values.spliterator(), false)
                    .map(x -> x.toString()).collect(joining(","));
            val.set(res);
            context.write(key, val);
        }
    }

}
