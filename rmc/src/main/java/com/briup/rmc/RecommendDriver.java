package com.briup.rmc;

import com.briup.rmc.coMatrix.*;
import com.briup.rmc.coMatrix.CheckDriver.Key;
import com.briup.rmc.coMatrix.CheckDriver.KeyComparator;
import com.briup.rmc.coMatrix.CheckDriver.KeyPartitioner;
import com.briup.rmc.coMatrix.CheckDriver.UserResult;
import com.briup.rmc.coMatrix.ItemCooccurenceList.ListMapper;
import com.briup.rmc.coMatrix.ItemCooccurenceList.ListReducer;
import com.briup.rmc.coMatrix.ResultSort.ResultComparator;
import com.briup.rmc.coMatrix.ResultSort.ResultDB;
import com.briup.rmc.coMatrix.ResultSort.ResultPartitioner;
import com.briup.rmc.common.Preference;
import com.briup.rmc.common.VectorWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class RecommendDriver extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new RecommendDriver(), args));
    }

    public ControlledJob getCoocJob(Configuration conf) throws Exception {
        Path in = new Path(conf.get("input"));
        Path out = new Path("temp/cocc");

        Job job = Job.getInstance(conf);
        job.setJarByClass(this.getClass());
        job.setJobName("cooc job");

        job.setMapperClass(ItemCooccurence.CoocMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setReducerClass(ItemCooccurence.CoocReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        TextInputFormat.addInputPath(job, in);
        SequenceFileOutputFormat.setOutputPath(job, out);

        ControlledJob cj = new ControlledJob(job.getConfiguration());
        cj.setJob(job);
        return cj;
    }

    public ControlledJob getMatrixJob(Configuration conf) throws Exception {
        Path in = new Path("temp/cocc");
        Path out = new Path("temp/matrix");

        Job job = Job.getInstance(conf);
        job.setJarByClass(this.getClass());
        job.setJobName("Matrix Job");

        job.setMapperClass(ItemCooccurenceMatrix.ListMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setCombinerClass(IntSumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        TextInputFormat.addInputPath(job, in);
        TextOutputFormat.setOutputPath(job, out);

        ControlledJob cj = new ControlledJob(job.getConfiguration());
        cj.setJob(job);
        return cj;
    }

    public ControlledJob getMatrixListJob(Configuration conf) throws Exception {
        Path in = new Path("temp/matrix");
        Path out = new Path("temp/matrixList");

        Job job = Job.getInstance(conf);
        job.setJarByClass(this.getClass());
        job.setJobName("Matrix List Job");

        job.setMapperClass(ListMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Preference.class);

        job.setReducerClass(ListReducer.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(VectorWritable.class);

        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        KeyValueTextInputFormat.addInputPath(job, in);
        TextOutputFormat.setOutputPath(job, out);

        ControlledJob cj = new ControlledJob(job.getConfiguration());
        cj.setJob(job);
        return cj;
    }

    public ControlledJob getItemListJob(Configuration conf) throws Exception {
        Path in = new Path(conf.get("input"));
        Path out = new Path("temp/itemList");

        Job job = Job.getInstance(conf);
        job.setJarByClass(this.getClass());
        job.setJobName("Item List Job");

        job.setMapperClass(ItemPreferenceList.ListMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Preference.class);

        job.setReducerClass(ItemPreferenceList.ListReducer.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(VectorWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        TextInputFormat.addInputPath(job, in);
        TextOutputFormat.setOutputPath(job, out);
        ControlledJob cj = new ControlledJob(job.getConfiguration());
        cj.setJob(job);
        return cj;
    }

    public ControlledJob getUserListJob(Configuration conf) throws Exception {
        Path user = new Path("temp/itemList");
        Path item = new Path("temp/matrixList");
        Path out = new Path("temp/userList");

        Job job = Job.getInstance(conf);
        job.setJarByClass(this.getClass());
        job.setJobName("recommend list job");

        MultipleInputs.addInputPath(job, user, TextInputFormat.class, MatrixVectorMultiple.ItemUserMapper.class);
        MultipleInputs.addInputPath(job, item, TextInputFormat.class, MatrixVectorMultiple.ItemItemMapper.class);
        job.setMapOutputKeyClass(MatrixVectorMultiple.PreferenceOrCooccurence.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, out);
        job.setPartitionerClass(MatrixVectorMultiple.ItemPartitioner.class);
        job.setGroupingComparatorClass(MatrixVectorMultiple.ItemComparator.class);

        job.setReducerClass(MatrixVectorMultiple.MultpleReducer.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(VectorWritable.class);

        ControlledJob cj = new ControlledJob(job.getConfiguration());
        cj.setJob(job);
        return cj;
    }

    public ControlledJob getVectJob(Configuration conf) throws IOException {
        Path in = new Path("temp/userList");
        Path out = new Path("temp/userVect");

        Job job = Job.getInstance(conf);
        job.setJarByClass(this.getClass());
        job.setJobName("recommend vector");

        job.setMapperClass(RecommendVector.SumMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Preference.class);

        job.setReducerClass(RecommendVector.SumReducer.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(VectorWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        TextInputFormat.addInputPath(job, in);
        TextOutputFormat.setOutputPath(job, out);
        ControlledJob cj = new ControlledJob(job.getConfiguration());
        cj.setJob(job);
        return cj;
    }

    public ControlledJob getCheckJob(Configuration conf) throws IOException {
        Path input = new Path(conf.get("input"));
        Path vector = new Path("temp/userVect");
        Path out = new Path(conf.get("output"));
        Job job = Job.getInstance(conf);
        job.setJarByClass(this.getClass());
        job.setJobName("recommend result");

        MultipleInputs.addInputPath(job, input, TextInputFormat.class, CheckDriver.UserPreference.class);
        MultipleInputs.addInputPath(job, vector, TextInputFormat.class, CheckDriver.UserRecommend.class);
        job.setMapOutputKeyClass(Key.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, out);

        job.setPartitionerClass(KeyPartitioner.class);
        job.setGroupingComparatorClass(KeyComparator.class);

        job.setReducerClass(UserResult.class);
        job.setOutputKeyClass(Key.class);
        job.setOutputValueClass(DoubleWritable.class);
        ControlledJob cj = new ControlledJob(job.getConfiguration());
        cj.setJob(job);
        return cj;

    }

    public ControlledJob getResultJob(Configuration conf) throws IOException {

        Path input = new Path(conf.get("output"));

        Job job = Job.getInstance(conf);
        job.setJarByClass(this.getClass());
        job.setJobName("result to db");

        TextInputFormat.addInputPath(job, input);

        DBConfiguration.configureDB(
                job.getConfiguration(),
                "com.mysql.jdbc.Driver",
                "jdbc:mysql://172.16.0.100:3306/hadoop",
                "hadoop", "hadoop");
        DBOutputFormat.setOutput(job,
                "recommend_tbl", "userId", "itemId",
                "preference");

        job.setMapperClass(ResultSort.ResultMapper.class);
        job.setMapOutputKeyClass(Preference.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setSortComparatorClass(ResultSort.ValueComparator.class);
        job.setPartitionerClass(ResultPartitioner.class);
        job.setGroupingComparatorClass(ResultComparator.class);

        job.setReducerClass(ResultSort.ResultReducer.class);
        job.setOutputKeyClass(ResultDB.class);
        job.setOutputValueClass(NullWritable.class);

        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(DBOutputFormat.class);

        ControlledJob cj = new ControlledJob(job.getConfiguration());
        cj.setJob(job);
        return cj;

    }

    @Override
    public int run(String[] in) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = getConf();
        ControlledJob coocJob = this.getCoocJob(conf);
        ControlledJob matrixJob = this.getMatrixJob(conf);
        ControlledJob matrixListJob = this.getMatrixListJob(conf);
        ControlledJob itemListJob = this.getItemListJob(conf);
        ControlledJob userListJob = this.getUserListJob(conf);
        ControlledJob userVectJob = this.getVectJob(conf);
        ControlledJob checkJob = this.getVectJob(conf);
        ControlledJob resultJob = this.getResultJob(conf);

        matrixJob.addDependingJob(coocJob);
        matrixListJob.addDependingJob(matrixJob);
        userListJob.addDependingJob(itemListJob);
        userListJob.addDependingJob(matrixListJob);
        userVectJob.addDependingJob(userListJob);
        checkJob.addDependingJob(userVectJob);
        resultJob.addDependingJob(checkJob);

        JobControl jc = new JobControl("recommend");
        jc.addJob(coocJob);
        jc.addJob(matrixJob);
        jc.addJob(matrixListJob);
        jc.addJob(itemListJob);
        jc.addJob(userListJob);
        jc.addJob(userVectJob);
        jc.addJob(checkJob);
        jc.addJob(resultJob);

        Thread jcThread = new Thread(jc);
        jcThread.start();

        while (true) {
            for (ControlledJob j :
                    jc.getSuccessfulJobList()) {
                j.getJob().monitorAndPrintJob();

            }
            for (ControlledJob j :
                    jc.getFailedJobList()) {
                j.getJob().monitorAndPrintJob();

            }
            if (jc.allFinished()) break;
        }

        return 0;
    }

}
