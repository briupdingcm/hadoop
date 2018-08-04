package com.briup.mr.type;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class YearStation
        implements WritableComparable<YearStation> {
    private IntWritable year;
    private Text stationId;

    public YearStation() {
        year = new IntWritable();
        stationId = new Text();
    }

    public YearStation(YearStation ys) {
        year = new IntWritable();
        year.set(ys.year.get());
        stationId = new Text(ys.stationId);
    }

    @Override
    public void readFields(DataInput input)
            throws IOException {
        year.readFields(input);
        stationId.readFields(input);
    }

    @Override
    public void write(DataOutput output) throws IOException {
        year.write(output);
        stationId.write(output);
    }

    @Override
    public int compareTo(YearStation ys) {
        return (year.compareTo(ys.year) != 0) ?
                (year.compareTo(ys.year))
                : (stationId.compareTo(ys.stationId));
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof YearStation)) {
            return false;
        }
        YearStation ys = (YearStation) obj;
        return (year == ys.year) && (stationId.equals(ys.stationId));
    }

    @Override
    public int hashCode() {
        return Math.abs(year.hashCode() * 127 + stationId.hashCode());
    }

    @Override
    public String toString() {
        return year + "\t" + stationId;
    }

    public void set(IntWritable year, Text stationId) {
        set(year.get(), stationId.toString());
    }

    public void set(int year, String stationId) {
        this.year.set(year);
        this.stationId.set(stationId);
    }

    public IntWritable getYear() {
        return year;
    }

    public Text getStationId() {
        return stationId;
    }

}


