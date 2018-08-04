package com.briup.mr.conf;

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

public class Cooccurence
        extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        // TODO Auto-generated method stub
        Cooccurence prc = new Cooccurence();
        // TODO Auto-generated method stub
        System.exit(ToolRunner.run(prc, args));

    }

    @Override
    public int run(String[] arg0) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = getConf();
        conf.set("parser.class", "com.briup.mr.advance.conf.LineParser");
        Path input = new Path(conf.get("input"));
        Path output = new Path(conf.get("output"));

        Job job = Job.getInstance(conf);
        job.setJarByClass(Cooccurence.class);//设置job驱动类
        job.setJobName("Friend Cooccurence Counter");//设置job名字

        //给job设置mapper类及map方法产生的键值类型
        job.setMapperClass(CoMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //给job设置reducer类及reduce方法产生的键值类型
        job.setReducerClass(CoReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //设置文件的读取方式（文本文件），输出方式（文本文件）
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        //给job指定输入文件的路径和输出结果的路径
        TextInputFormat.addInputPath(job, input);
        TextOutputFormat.setOutputPath(job, output);

        //向集群提交作业
        return job.waitForCompletion(true) ? 0 : 1;
    }

    static class CoMapper
            extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Parser parser = null;
        private Text k = new Text();
        private IntWritable v = new IntWritable(1);

        @Override
        protected void setup(Mapper<LongWritable, Text, Text, IntWritable>.Context context) {
            Configuration conf = context.getConfiguration();
            try {
                parser = (Parser) Class.forName(
                        conf.get("parser.class")).newInstance();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            if (parser.parse(value)) {
                for (int i = 1; i < parser.getCount() - 1; i++) {
                    for (int j = i + 1; j < parser.getCount(); j++) {
                        k.set(getPair(parser.get(i),
                                parser.get(j)));
                        context.write(k, v);
                    }
                }
            }
			/*
			String[] items = value.toString().split(",");
			for(int i = 1; i < items.length - 1; i++){
				for(int j = i + 1; j < items.length; j++){
					k.set(getPair(items[i].trim(), items[j].trim()));
					context.write(k, v);
				}
			}
			*/
        }

        private String getPair(String first, String second) {
            if (first.compareTo(second) < 0)
                return first + "," + second;
            return second + "," + first;
        }
    }

    static class CoReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable counter = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int count = 0;
            for (IntWritable v : values) {
                count += v.get();
            }
            counter.set(count);
            context.write(key, counter);
        }
    }

}
