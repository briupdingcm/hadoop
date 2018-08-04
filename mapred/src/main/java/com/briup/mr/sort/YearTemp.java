package com.briup.mr.sort;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class YearTemp implements WritableComparable<YearTemp> {
    private IntWritable year = new IntWritable();
    private DoubleWritable temperature = new DoubleWritable();

    @Override
    public void readFields(DataInput in) throws IOException {
        year.readFields(in);
        temperature.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        year.write(out);
        temperature.write(out);
    }

    @Override
    public int compareTo(YearTemp o) {
        int res = year.compareTo(o.year);
        if (res != 0) return res;
        return temperature.compareTo(o.temperature);
    }


    public IntWritable getYear() {
        return year;
    }

    public void setYear(IntWritable year) {
        this.year.set(year.get());
    }

    public DoubleWritable getTemperature() {
        return temperature;
    }

    public void setTemperature(DoubleWritable temperature) {
        this.temperature.set(temperature.get());
    }

    public void set(int year, double temp) {
        this.year.set(year);
        this.temperature.set(temp);
    }

}
