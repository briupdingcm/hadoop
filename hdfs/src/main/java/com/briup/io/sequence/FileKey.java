package com.briup.io.sequence;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FileKey
        implements WritableComparable<FileKey> {
    private Text fileName = new Text();
    private LongWritable length = new LongWritable();

    @Override
    public void readFields(DataInput input) throws IOException {
        fileName.readFields(input);
        length.readFields(input);
    }

    @Override
    public void write(DataOutput output) throws IOException {
        fileName.write(output);
        length.write(output);
    }

    @Override
    public int compareTo(FileKey other) {
        if ((fileName.compareTo(other.fileName) == 0)
                && (length.compareTo(other.length) == 0)) {
            return 0;
        } else if ((fileName.compareTo(other.fileName) != 0)) {
            return fileName.compareTo(other.fileName);
        } else {
            return length.compareTo(other.length);
        }
    }

    public String getFileName() {
        return fileName.toString();
    }

    public void setFileName(String fileName) {
        this.fileName.set(fileName);
    }

    public long getLength() {
        return length.get();
    }

    public void setLength(long length) {
        this.length.set(length);
    }

    public String toString() {
        return fileName.toString() + ":" + length.get();
    }
}
