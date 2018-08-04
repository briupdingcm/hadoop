package com.briup.mr.inputFormat.split;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

public class FileRecordReader extends RecordReader<Text, BytesWritable> {
    private Text key = new Text();
    private BytesWritable value = new BytesWritable();
    private Configuration conf;
    private Iterator<String> iter;
    private int num = 0;
    private int total = 0;

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext context) {
        this.conf = context.getConfiguration();
        total = ((MultiFileSplit) inputSplit).getMaxLocation();
        iter = ((MultiFileSplit) inputSplit).getAllFiles().iterator();
    }

    @Override
    public boolean nextKeyValue() throws IOException {
        if (!iter.hasNext())
            return false;
        num++;
        Path path = new Path(iter.next());
        key.set(path.toString());
        FileSystem fs = path.getFileSystem(conf);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        InputStream is = fs.open(path);
        try {
            IOUtils.copyBytes(is, baos, conf, true);
            value.set(baos.toByteArray(), 0, baos.size());
        } finally {
            fs.close();
        }

        return true;
    }

    @Override
    public Text getCurrentKey() {
        return key;
    }

    @Override
    public BytesWritable getCurrentValue() {
        return value;
    }

    @Override
    public float getProgress() {
        return ((float) num) / total;
    }

    @Override
    public void close() {

    }

}
