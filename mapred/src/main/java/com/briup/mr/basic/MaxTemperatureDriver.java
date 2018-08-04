package com.briup.mr.basic;

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
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.briup.mr.common.ThrowingConsumer.throwingConsumerWrapper;
import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.maxBy;

/**
 * 计算每年的最高温度
 * Driver类：MaxTemperatureDriver类负责配置job对象
 * Mapper类：TemperatureMapper类负责从数据源中抽取年份和温度数据
 * Reducer类：TemperatureReducer负责从年份的所有温度值中找出最大值
 */
public class MaxTemperatureDriver extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new MaxTemperatureDriver(), args));
    }

    @Override
    public int run(String[] arg0) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = getConf();
        Path input = new Path(conf.get("input"));
        Path output = new Path(conf.get("output"));

        Job job = Job.getInstance(conf);
        job.setJarByClass(MaxTemperatureDriver.class);
        job.setJobName("max temperature(dingcm)");

        job.setMapperClass(TemperatureMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);


        job.setReducerClass(TemperatureReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        TextInputFormat.addInputPath(job, input);
        TextOutputFormat.setOutputPath(job, output);

        job.setNumReduceTasks(6);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class TemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Text year = new Text();//键对象
        private IntWritable temp = new IntWritable();//值对象
        private NcdcRecordParser parser = new NcdcRecordParser();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            /*parser.parse(value);
            if (parser.isValidTemperature()) {
                year.set(parser.getYear() + "");
                temp.set(parser.getAirTemperature());
                context.write(year, temp);
            }
            */
            parser.parse(() -> value.toString()).ifPresent(throwingConsumerWrapper(p -> {
                year.set(p.getYear() + "");
                temp.set(p.getAirTemperature());
                context.write(year, temp);

            }));
        }
    }

    public static class TemperatureReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable temp = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            Stream<IntWritable> stream = StreamSupport.stream(values.spliterator(), true);
            //寻找values中的最大值（最高温度）
            stream.collect(maxBy(comparing(x -> x.get()))).ifPresent(
                    throwingConsumerWrapper(v -> context.write(key, v)));

//            int m2 = stream.mapToInt(x -> x.get()).max().getAsInt();
//            int maxValue = stream.mapToInt(x -> x.get()).reduce(
//                    Integer.MIN_VALUE,
//                    (max, e) -> Math.max(e, max));

//            temp.set(maxValue);
//            context.write(key, temp);
        }

    }

}
