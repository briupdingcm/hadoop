package com.briup.mr.type;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class StationTemp implements Writable {
    private Text station = new Text();
    private IntWritable temp = new IntWritable();


    @Override
    public void write(DataOutput dataOutput) throws IOException {
        station.write(dataOutput);
        temp.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        station.readFields(dataInput);
        temp.readFields(dataInput);
    }

    public Text getStation() {
        return station;
    }

    public void setStation(Text station) {
        this.station = station;
    }

    public IntWritable getTemp() {
        return temp;
    }

    public void setTemp(IntWritable temp) {
        this.temp = temp;
    }

    @Override
    public String toString() {
        return station.toString() + "\t" + temp.toString();
    }
}
