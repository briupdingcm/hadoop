package com.briup.mr.inputFormat.reader.ysw;

import com.briup.mr.type.YearStation;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

public class YearStationInputFormat
        extends FileInputFormat<YearStation, IntWritable> {
    @Override
    public RecordReader<YearStation, IntWritable>
    createRecordReader(InputSplit split, TaskAttemptContext context)
            throws IOException {
        YearStationRecordReader ysrr =
                new YearStationRecordReader();
        ysrr.initialize(split, context);
        return ysrr;
    }

    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        CompressionCodec codec =
                new CompressionCodecFactory(context.getConfiguration()).getCodec(filename);
        return codec == null;
    }

}
