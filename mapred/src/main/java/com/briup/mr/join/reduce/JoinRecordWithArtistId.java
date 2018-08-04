package com.briup.mr.join.reduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Iterator;

import static com.briup.mr.common.ThrowingConsumer.throwingConsumerWrapper;

/**
 * Reduce-end join
 *
 * @author kevin
 */
public class JoinRecordWithArtistId extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new JoinRecordWithArtistId(), args));
    }

    @Override
    public int run(String[] arg0) throws Exception {
        Configuration conf = getConf();
        Path inputArtist = new Path(conf.get("artistInput"));
        Path inputRecord = new Path(conf.get("recordInput"));
        Path output = new Path(conf.get("output"));

        Job job = Job.getInstance(conf);
        job.setJarByClass(this.getClass());
        job.setJobName("join user and artist");

        MultipleInputs.addInputPath(job, inputArtist, TextInputFormat.class, JoinArtistMapper.class);
        MultipleInputs.addInputPath(job, inputRecord, TextInputFormat.class, JoinRecordMapper.class);

        TextOutputFormat.setOutputPath(job, output);

        job.setPartitionerClass(KeyPartitioner.class);
        job.setGroupingComparatorClass(TextPairComparator.class);

        job.setMapOutputKeyClass(TextTuple.class);
        job.setReducerClass(JoinReducer.class);

        job.setOutputKeyClass(NullWritable.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    static class JoinArtistMapper extends Mapper<LongWritable, Text, TextTuple, Text> {
        private ArtistMetaParser parser = new ArtistMetaParser();
        private TextTuple k = new TextTuple();
        private Text v = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			/*
			parser.parse(value);
			if (parser.isValid()) {
				k.set(parser.getArtistId(), "0");
				v.set(parser.getArtistName()+","+parser.getDate());
				context.write(k, value);
			}*/
            parser.parse(() -> value.toString()).ifPresent(throwingConsumerWrapper(amp -> {
                //ArtistMetaParser amp = (ArtistMetaParser)p;
                k.set(amp.getArtistId(), "0");
                v.set(amp.getArtistName() + "," + amp.getDate());
                context.write(k, value);
            }));
        }
    }

    static class JoinRecordMapper extends Mapper<LongWritable, Text, TextTuple, Text> {
        private ArtistRecorderParser parser = new ArtistRecorderParser();
        private TextTuple k = new TextTuple();
        private Text v = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			/*parser.parse(value);
			if (parser.isValid()) {
				k.set(parser.getArtistId(), "1");
				v.set(parser.getDate()+","+parser.getCounter());
				context.write(k, v);
			}*/
            parser.parse(() -> value.toString()).ifPresent(throwingConsumerWrapper(arp -> {
                //ArtistRecorderParser arp = (ArtistRecorderParser)p;
                k.set(arp.getArtistId(), "1");
                v.set(arp.getDate() + "," + arp.getCounter());
                context.write(k, value);
            }));
        }
    }

    static class JoinReducer extends Reducer<TextTuple, Text, NullWritable, Text> {
        private Text v = new Text();
        private NullWritable k = NullWritable.get();

        @Override
        protected void reduce(TextTuple key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            Iterator<Text> iter = values.iterator();
            Text artistInfo = new Text(iter.next());
            while (iter.hasNext()) {
                Text recordInfo = iter.next();
                v.set(artistInfo.toString() + "," + recordInfo.toString());
                context.write(k, v);
            }

//			Stream<Text> stream = StreamSupport.stream(values.spliterator(), false);
//			stream.findFirst().ifPresent((Text f) -> {
//				StreamSupport.stream(values.spliterator(), false).forEach(throwingConsumerWrapper(e -> {
//					v.set(f.toString() + "," + e.toString());
//					context.write(k, v);
//				}));
//			});
//			stream.reduce((f, s) -> {
//				v.set(f.toString() + "," + s.toString());
//				try {
//					context.write(k, v);
//				} catch (Exception e) {
//					throw new RuntimeException(e);//e.printStackTrace();
//				}
//				return f;
//			});
        }

    }

}
