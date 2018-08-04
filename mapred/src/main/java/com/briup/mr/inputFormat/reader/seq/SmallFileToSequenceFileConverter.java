package com.briup.mr.inputFormat.reader.seq;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class SmallFileToSequenceFileConverter extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new SmallFileToSequenceFileConverter(), args));
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();

        Path in = new Path(conf.get("input"));
        Path out = new Path(conf.get("output"));

        Job job = Job.getInstance(conf);
        job.setJarByClass(getClass());
        job.setJobName("small file sequence file");

        job.setInputFormatClass(WholeInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);

        job.setMapperClass(SequenceFileMapper.class);

        WholeInputFormat.addInputPath(job, in);
        SequenceFileOutputFormat.setOutputPath(job, out);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    static class SequenceFileMapper extends Mapper<NullWritable, BytesWritable, Text, BytesWritable> {
        private Text filenameKey;

        @Override
        protected void map(NullWritable key, BytesWritable value,
                           Mapper<NullWritable, BytesWritable, Text, BytesWritable>.Context context)
                throws IOException, InterruptedException {
            context.write(filenameKey, value);
        }

        @Override
        protected void setup(Mapper<NullWritable, BytesWritable, Text, BytesWritable>.Context context) {
            FileSplit split = (FileSplit) context.getInputSplit();
            filenameKey = new Text(split.getPath().toString());
        }

    }

}
