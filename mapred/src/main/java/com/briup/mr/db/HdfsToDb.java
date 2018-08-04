package com.briup.mr.db;

import com.briup.mr.basic.NcdcRecordParser;
import com.briup.mr.type.YearStation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.briup.mr.common.ThrowingConsumer.throwingConsumerWrapper;

/**
 * 从气象数据集中抽取每个气象站每年的最高温度并计入mysql数据库
 */
public class HdfsToDb extends Configured implements Tool {
    public static void main(String... args) throws Exception {
        System.exit(ToolRunner.run(new HdfsToDb(), args));
    }

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Path input = new Path(conf.get("input"));

        Job job = Job.getInstance(conf);
        job.setJarByClass(HdfsToDb.class);
        job.setJobName("max temperature by year station");

        TextInputFormat.addInputPath(job, input);

        DBConfiguration.configureDB(
                job.getConfiguration(),
                "com.mysql.jdbc.Driver",
                "jdbc:mysql://172.16.0.100:3306/hadoop",
                "hadoop", "hadoop");
        //insert into station_tbl(year,station,temperature) values(?, ?, ?)
        DBOutputFormat.setOutput(job, "station_tbl",
                "year", "station", "temperature");

        job.setMapperClass(MaxTemperatureMapper.class);
        job.setMapOutputKeyClass(YearStation.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(MaxTemperatureReducer.class);
        job.setOutputKeyClass(YearStationDB.class);
        job.setOutputValueClass(NullWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(DBOutputFormat.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    static class MaxTemperatureMapper extends Mapper<LongWritable, Text, YearStation, IntWritable> {
        private NcdcRecordParser parser = new NcdcRecordParser();
        private YearStation k = new YearStation();
        private IntWritable v = new IntWritable();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//			parser.parse(value);
//			if (parser.isValidTemperature()) {
//				k.set(parser.getYear(), parser.getStationId());
//				v.set(parser.getAirTemperature());
//				context.write(k, v);
//			}
            parser.parse(() -> value.toString()).ifPresent(throwingConsumerWrapper(p -> {
                //NcdcRecordParser p = (NcdcRecordParser)p1;
                k.set(p.getYear(), p.getStationId());
                v.set(p.getAirTemperature());
                context.write(k, v);

            }));
        }
    }

    static class MaxTemperatureReducer extends Reducer<YearStation, IntWritable, YearStationDB, NullWritable> {
        private YearStationDB k = new YearStationDB();

        public void reduce(YearStation key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            //计算最大值
            Stream<Integer> stream = StreamSupport.stream(values.spliterator(), false).map(x -> x.get());
            int maxValue = stream.reduce(0,
                    (max, curr) -> Math.max(max, curr));

            k.setYear(key.getYear().get());
            k.setStationId(key.getStationId().toString());
            k.setTemperature(maxValue);
            context.write(k, NullWritable.get());
        }
    }
}
