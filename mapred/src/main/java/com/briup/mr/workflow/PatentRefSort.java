package com.briup.mr.workflow;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;


/**
 * yarn jar target/mapred-0.0.1.jar
 * com.briup.workflow.jc.PatentRefSort
 * -D input=/data/patent/cite75_99.txt
 * -D out=patent
 *
 * @author kevin
 */
public class PatentRefSort
        extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new PatentRefSort(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] arg0) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = getConf();
        Path temp = new Path(
                (conf.get("temp") == null ?
                        "temp" : conf.get("temp")));

        conf.set(
                "mapreduce.input.keyvaluelinerecordreader.key.value.separator",
                ",");

        Job job1 = Job.getInstance(conf);
        job1.setJarByClass(this.getClass());
        job1.setJobName("patent reference count");

        ChainMapper.addMapper(job1,
                InverseMapper.class,
                Text.class, Text.class,
                Text.class, Text.class, conf);
        ChainMapper.addMapper(job1,
                CounterMapper.class,
                Text.class, Text.class,
                Text.class, IntWritable.class, conf);

        job1.setReducerClass(IntSumReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        job1.setInputFormatClass(
                KeyValueTextInputFormat.class);
        job1.setOutputFormatClass(
                SequenceFileOutputFormat.class);

        KeyValueTextInputFormat.addInputPath(
                job1, new Path(conf.get("input")));
        SequenceFileOutputFormat.setOutputPath(job1,
                temp);


        Job job2 = Job.getInstance(conf);
        job2.setJarByClass(this.getClass());
        job2.setJobName("patent reference sort");

        job2.setMapperClass(InverseMapper.class);
        job2.setMapOutputKeyClass(IntWritable.class);
        job2.setMapOutputValueClass(Text.class);
        ChainReducer.setReducer(job2,
                Reducer.class,
                IntWritable.class, Text.class,
                IntWritable.class, Text.class, conf);
        ChainReducer.addMapper(job2,
                InverseMapper.class,
                IntWritable.class, Text.class,
                Text.class, IntWritable.class, conf);

        job2.setInputFormatClass(
                SequenceFileInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        SequenceFileInputFormat.addInputPath(job2,
                temp);
        TextOutputFormat.setOutputPath(job2,
                new Path(conf.get("output")));

        ControlledJob cJob1 = new ControlledJob(
                job1.getConfiguration());
        cJob1.setJob(job1);

        ControlledJob cJob2 = new ControlledJob(
                job2.getConfiguration());
        cJob2.setJob(job2);

        cJob2.addDependingJob(cJob1);

        JobControl jobControl =
                new JobControl("patent process");
        jobControl.addJob(cJob1);
        jobControl.addJob(cJob2);

        Thread jcThread = new Thread(jobControl);
        jcThread.start();

        while (true) {
            for (ControlledJob j :
                    jobControl.getRunningJobList()) {
                j.getJob().monitorAndPrintJob();
            }
            if (jobControl.allFinished()) break;
        }
        return 0;
    }

    static class CounterMapper
            extends Mapper<Text, Text, Text, IntWritable> {
        private IntWritable one = new IntWritable(1);

        @Override
        protected void map(Text key, Text value, Context context)
                throws IOException, InterruptedException {
            context.write(key, one);
        }
    }
}
