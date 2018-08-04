package com.briup.mr.app.score;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

import static com.briup.mr.common.ThrowingConsumer.throwingConsumerWrapper;

public class ScoreAverageDriver extends Configured implements Tool {
    private static ScoreParser parser = new ScoreParser();

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new ScoreAverageDriver(), args));
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Job job = Job.getInstance(conf);
        job.setJarByClass(ScoreAverageDriver.class);

        // 设置输入目录和输出目录
        FileInputFormat.addInputPath(job, new Path(conf.get("input")));
        FileOutputFormat.setOutputPath(job, new Path(conf.get("output")));

        // 设置Mapper和Reducer
        job.setMapperClass(ScoreAverageMapper.class);
        job.setReducerClass(ScoreAverageReducer.class);

        // 设置输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 作业调度
        return job.waitForCompletion(true) ? 0 : 1;
    }

    static class ScoreAverageMapper extends Mapper<LongWritable, Text, Text, Text> {
        private static Text outKey = new Text();

        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
//			if(parser.parse(value)){
//				outKey.set(parser.getName());
//				context.write(outKey, value);
//			}
            parser.parse(() -> value.toString()).ifPresent(throwingConsumerWrapper(p1 -> {
                ScoreParser p = p1;
                outKey.set(p.getName());
                context.write(outKey, value);
            }));
        }
    }

    static class ScoreAverageReducer extends Reducer<Text, Text, Text, Text> {

        private static Text outValue = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            StringBuffer sb = new StringBuffer();
            double sum = 0;
            for (Text text : values) {
                parser.parse(text);
                if (parser.isValid()) {
                    String id = parser.getId();
                    String score = parser.getScore();
                    if ("a".equals(id)) {
                        sb.append("语文:" + score + "\t");
                    } else if ("b".equals(id)) {
                        sb.append("英语:" + score + "\t");
                    } else if ("c".equals(id)) {
                        sb.append("数学:" + score + "\t");
                    }
                    sum += Double.parseDouble(score);

                }
            }
            sb.append("总分:" + sum + "\t平均分:" + (sum / 3));
            outValue.set(sb.toString());
            context.write(key, outValue);

//			values.forEach(text -> {
//				parser.parse(() -> text.toString()).ifPresent(p1 -> {
//					ScoreParser p = (ScoreParser)p1;
//					String id = p.getId();
//					String score = p.getScore();
//					if ("a".equals(id)) {
//						sb.append("语文:" + score + "\t");
//					} else if ("b".equals(id)) {
//						sb.append("英语:" + score + "\t");
//					} else if ("c".equals(id)) {
//						sb.append("数学:" + score + "\t");
//					}
//					sum += Double.parseDouble(score);
//
//				});
        }
    }
}
