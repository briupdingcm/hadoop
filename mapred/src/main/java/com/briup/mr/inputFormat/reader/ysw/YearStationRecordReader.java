package com.briup.mr.inputFormat.reader.ysw;

import com.briup.mr.basic.NcdcRecordParser;
import com.briup.mr.type.YearStation;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import java.io.IOException;

public class YearStationRecordReader
        extends RecordReader<YearStation, IntWritable> {
    private LineRecordReader reader = new LineRecordReader(); // proxy

    private NcdcRecordParser parser = new NcdcRecordParser();
    private YearStation key = new YearStation();
    private IntWritable value = new IntWritable();

    @Override
    public YearStation getCurrentKey() {
        return key;
    }

    @Override
    public IntWritable getCurrentValue() {
        return value;
    }

    @Override
    public float getProgress() throws IOException {
        return reader.getProgress();
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException {
        reader.initialize(split, context);
    }

    @Override
    public boolean nextKeyValue() throws IOException {
        do {
            if (!reader.nextKeyValue())
                return false;
            Text value = reader.getCurrentValue();
            parser.parse(value);
        } while (parser.isValidTemperature());
        key.set(parser.getYear(), parser.getStationId());
        value.set(parser.getAirTemperature());
        return true;
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }
}
