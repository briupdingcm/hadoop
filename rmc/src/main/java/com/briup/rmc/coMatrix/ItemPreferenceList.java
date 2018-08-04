package com.briup.rmc.coMatrix;

import com.briup.rmc.common.Preference;
import com.briup.rmc.common.RecordParser;
import com.briup.rmc.common.VectorWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class ItemPreferenceList extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        // TODO Auto-generated method stub
        System.exit(ToolRunner.run(new ItemPreferenceList(), args));

    }

    @Override
    public int run(String[] arg0) throws Exception {
        Configuration conf = getConf();
        Path in = new Path(conf.get("input"));
        Path out = new Path(conf.get("output"));

        Job job = Job.getInstance(conf);
        job.setJarByClass(this.getClass());
        job.setJobName("Item User List");

        job.setMapperClass(ListMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Preference.class);

        job.setReducerClass(ListReducer.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(VectorWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setCompressOutput(job, true);
        TextOutputFormat.setOutputCompressorClass(job, BZip2Codec.class);

        SequenceFileInputFormat.addInputPath(job, in);
        SequenceFileOutputFormat.setOutputPath(job, out);

        //job.submit();

        //return 0;
        return job.waitForCompletion(true) ? 0 : 1;//0;
    }

    public static class ListMapper
            extends Mapper<LongWritable, Text, LongWritable, Preference> {
        private RecordParser parser = new RecordParser();
        private LongWritable k = new LongWritable();
        private Preference pref = new Preference();

        @Override
        protected void map(LongWritable key, Text value,
                           Mapper<LongWritable, Text, LongWritable, Preference>.Context context)
                throws IOException, InterruptedException {
            if (parser.parse(value)) {
                k.set(parser.getItemId());
                pref.setId(parser.getUserId());
                pref.setValue(parser.getPreference());
                context.write(k, pref);
            }
        }

    }

    public static class ListReducer extends Reducer<LongWritable, Preference, LongWritable, VectorWritable> {
        private VectorWritable v = new VectorWritable();

        @Override
        protected void reduce(LongWritable key,
                              Iterable<Preference> prefs,
                              Context context)
                throws IOException, InterruptedException {
            v.clear();
            for (Preference pref : prefs) {
                v.add(new Preference(pref));
            }
            context.write(key, v);
        }

    }

}
