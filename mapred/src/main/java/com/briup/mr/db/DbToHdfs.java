package com.briup.mr.db;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class DbToHdfs extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        // TODO Auto-generated method stub
        System.exit(ToolRunner.run(new DbToHdfs(), args));
    }

    @Override
    public int run(String[] arg0) throws Exception {
        Configuration conf = getConf();
        Path output = new Path(conf.get("output"));
        Job job = Job.getInstance(conf);
        job.setJarByClass(DbToHdfs.class);
        job.setJobName("mysql to hdfs");

        DBConfiguration.configureDB(
                job.getConfiguration(),
                "com.mysql.jdbc.Driver",
                "jdbc:mysql://172.16.0.100:3306/hadoop",
                "hadoop", "hadoop");
        DBInputFormat.setInput(job,
                YearStationDB.class,
                "station_tbl", // table  name
                "year > 2000", // where year > 2000
                "temperature", // key order by temperature
                "year", "station", "temperature"// column name
        );

        TextOutputFormat.setOutputPath(job, output);

        // select year, station, temperature from
        // station_tbl where year > 2000 order by temperature

        job.setMapperClass(DBInputMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        // 设置输出格式
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        // 设置输入格式
        job.setInputFormatClass(DBInputFormat.class);

        // 提交作业
        return job.waitForCompletion(true) ? 0 : 1;
    }

    static class DBInputMapper extends Mapper<LongWritable, YearStationDB, LongWritable, Text> {
        private Text outValue = new Text();

        @Override
        protected void map(LongWritable key, YearStationDB value, Context context)
                throws IOException, InterruptedException {
            outValue.set(value.toString());
            context.write(key, outValue);
        }
    }
}
