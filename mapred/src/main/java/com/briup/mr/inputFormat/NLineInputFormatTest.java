package com.briup.mr.inputFormat;

import com.briup.mr.basic.PatentRecordParser;
import com.briup.mr.basic.PatentReferenceCounter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.briup.mr.common.ThrowingConsumer.throwingConsumerWrapper;

public class NLineInputFormatTest extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new NLineInputFormatTest(), args));

    }

    @Override
    public int run(String[] arg0) throws Exception {
        Configuration conf = getConf();
        Path input = new Path(conf.get("input"));
        Path output = new Path(conf.get("output"));

        // 构建job对象，并设置驱动类名和job名
        Job job = Job.getInstance(conf);
        job.setJarByClass(PatentReferenceCounter.class);// 设置job驱动类
        job.setJobName("Patent Reference Counter");
        // 设置job名字

        // 给job设置mapper类及map方法输出的键值类型
        job.setMapperClass(PatentMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 给job设置reducer类及reduce方法输出的键值类型
        job.setReducerClass(PatentReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 设置文件的读取方式（文本文件），输出方式（文本文件）
        job.setInputFormatClass(NLineInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        //每lineNum行记录分成一个输入分片
        NLineInputFormat.setNumLinesPerSplit(job,
                conf.getInt("lineNum", 50000));
        // 给job指定输入文件的路径和输出结果的路径
        NLineInputFormat.addInputPath(job, input);
        TextOutputFormat.setOutputPath(job, output);

        // 向集群提交作业
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class PatentMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private PatentRecordParser parser = new PatentRecordParser();
        private Text patentID = new Text();
        private IntWritable one = new IntWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // 分割读入的一行
//			parser.parse(value);
//			if (parser.isValid()) {
//				// 构建键值对象
//				patentID.set(parser.getRefPatentId());
//				// 传递给MR框架
//				context.write(patentID, one);
//			}
            parser.parse(() -> value.toString()).ifPresent(throwingConsumerWrapper(p -> {
                PatentRecordParser prp = p;
                patentID.set(prp.getRefPatentId());
                context.write(patentID, one);
            }));
        }
    }

    static class PatentReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable counter = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            // 由map递交给MR框架的数据按照键分组后传递到reduce任务，values即为分组结果，key为这一组的键
//			int count = 0;
//			for (IntWritable v : values) {
//				count += v.get();
//			}
//			counter.set(count);
//			context.write(key, counter);
            Stream<IntWritable> stream = StreamSupport.stream(values.spliterator(), false);
            int count = stream.mapToInt(x -> x.get()).reduce(0,
                    (sum, curr) -> {
                        return sum + curr;
                    });

            counter.set(count);
            context.write(key, counter);
        }
    }

}
