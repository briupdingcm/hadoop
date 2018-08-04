package com.briup.mr.type;

import com.briup.mr.basic.NcdcRecordParser;
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
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.briup.mr.common.ThrowingConsumer.throwingConsumerWrapper;
import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.maxBy;

public class MTYS2 extends Configured implements Tool {


    public static void main(String... args) throws Exception {
        System.exit(ToolRunner.run(new MTYS2(), args));
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        Path input = new Path(conf.get("input"));
        Path output = new Path(conf.get("output"));

        Job job = Job.getInstance(conf);
        job.setJarByClass(MTYS2.class);
        job.setJobName("max temperature(dingcm)");

        job.setMapperClass(TemperatureMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(StationTemp.class);

        job.setReducerClass(MaxTemperatureReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(StationTemp.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        TextInputFormat.addInputPath(job, input);
        TextOutputFormat.setOutputPath(job, output);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class TemperatureMapper extends Mapper<LongWritable, Text, Text, StationTemp> {
        private Text year = new Text();//键对象
        private StationTemp temp = new StationTemp();//值对象
        private NcdcRecordParser parser = new NcdcRecordParser();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            parser.parse(() -> value.toString()).ifPresent(throwingConsumerWrapper(p -> {
                year.set(p.getYear() + "");
                temp.setTemp(new IntWritable(p.getAirTemperature()));
                temp.setStation(new Text(p.getStationId()));
                context.write(year, temp);

            }));
        }
    }

    static class MaxTemperatureReducer extends Reducer<Text, StationTemp, Text, StationTemp> {

        public void reduce(Text key, Iterable<StationTemp> values, Context context)
                throws IOException, InterruptedException {
           // values.forEach(throwingConsumerWrapper(p -> context.write(key, p)));
            Stream<StationTemp> stream = StreamSupport.stream(values.spliterator(), false);
            Map<Text, Optional<StationTemp>> group = stream.collect(
                    groupingBy(StationTemp::getStation, maxBy(comparing(st -> st.getTemp().get()))));
            group.entrySet().stream().forEach(x -> {
                x.getValue().ifPresent(throwingConsumerWrapper(y-> context.write(key, y)));
            });
        }
    }
}
