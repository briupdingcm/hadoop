package com.briup.mr.app.index;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static com.briup.mr.common.ThrowingConsumer.throwingConsumerWrapper;
import static java.util.stream.Collectors.counting;
import static java.util.stream.Collectors.groupingBy;

public class InvertedIndex extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        // TODO Auto-generated method stub
        System.exit(ToolRunner.run(new InvertedIndex(), args));
    }

    @Override
    public int run(String[] arg0) throws Exception {
        Configuration conf = getConf();
        Path input = new Path(conf.get("input"));
        Path output = new Path(conf.get("output"));

        Job job = Job.getInstance(conf);
        job.setJarByClass(InvertedIndex.class);
        job.setJobName("inverted index");

        job.setMapperClass(IndexMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(IndexReducer.class);
        // job.setNumReduceTasks(0);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        TextInputFormat.addInputPath(job, input);
        TextOutputFormat.setOutputPath(job, output);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    static class IndexMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text word = new Text();
        private Text file = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//			StringTokenizer st =
//					new StringTokenizer(value.toString(), " ");
//			String item = null;
//			while (st.hasMoreTokens()) {
//				item = st.nextToken();
//				if (item.trim().length() >= 1) {
//					word.set(item.trim());
//					context.write(word, file);
//				}
//			}
            Collections.list(new StringTokenizer(value.toString(), " ")).stream().map(x -> ((String) x).trim())
                    .filter(item -> item.length() > 1).forEach(throwingConsumerWrapper(item -> {
                word.set(item.trim());
                context.write(word, file);
            }));
        }

        @Override
        protected void setup(Context context) {
            String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
            file.set(fileName);
        }

    }

    static class IndexReducer extends Reducer<Text, Text, Text, Text> {
        private Map<String, Integer> index = new HashMap<String, Integer>();
        private Text v = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//			index.clear();
//			// 计算每个文件中单词出现的次数
//			for (Text file : values) {
//				String fileName = file.toString();
//				if (index.get(fileName) != null) {
//					index.put(fileName, 1 + index.get(fileName));
//				} else {
//					index.put(fileName, new Integer(1));
//				}
//			}
//			StringBuffer sb = new StringBuffer();
//			for (Entry<String, Integer> entry : index.entrySet()) {
//				sb.append("," + entry.getKey().toString() + ":" + entry.getValue());
//			}
//			v.set(sb.toString());
//			context.write(key, v);

            String res = StreamSupport.stream(values.spliterator(), false)
                    .collect(groupingBy(Text::toString, counting()))
                    .entrySet().stream().map(e -> e.getKey() + ":" + e.getValue())
                    .collect(Collectors.joining(",", "[", "]"));
            v.set(res);
            context.write(key, v);

        }
    }

}
