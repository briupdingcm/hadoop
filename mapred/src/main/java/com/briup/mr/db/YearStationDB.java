package com.briup.mr.db;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class YearStationDB
        implements DBWritable,
        WritableComparable<YearStationDB> {
    private int year;
    private String stationId;
    private int temperature;

    public YearStationDB() {
    }

    public YearStationDB(int year, String stationId,
                         int temperature) {
        this.year = year;
        this.stationId = stationId;
        this.temperature = temperature;
    }

    public int getTemperature() {
        return temperature;
    }

    public void setTemperature(int temperature) {
        this.temperature = temperature;
    }

    @Override
    public void readFields(ResultSet rs)
            throws SQLException {
        if (rs == null) return;
        year = rs.getInt(1);
        stationId = rs.getString(2);
        temperature = rs.getInt(3);
    }

    @Override
    public void write(PreparedStatement ps)
            throws SQLException {
        ps.setInt(1, year);
        ps.setString(2, stationId);
        ps.setInt(3, temperature);
    }

    @Override
    public void readFields(DataInput input)
            throws IOException {
        year = input.readInt();
        stationId = input.readUTF();
        temperature = input.readInt();
    }

    @Override
    public void write(DataOutput output)
            throws IOException {
        output.writeInt(year);
        output.writeUTF(stationId);
        output.writeInt(temperature);
    }

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public String getStationId() {
        return stationId;
    }

    public void setStationId(String stationId) {
        this.stationId = stationId;
    }

    @Override
    public String toString() {
        return getYear() + "," + getStationId() + "," + this.temperature;
    }

    @Override
    public int compareTo(YearStationDB ys) {
        return (year != ys.year) ?
                ((year > ys.year) ? 1 : -1)
                : (stationId.compareTo(ys.stationId));

    }
}
