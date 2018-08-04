package com.briup.mr.sort;

import com.briup.mr.common.Parser;
import org.apache.hadoop.io.Text;

public class YearTempParser implements Parser<String, YearTempParser> {
    private int year;
    private String station;
    private double temperature;

    private boolean valid = false;

    public void parse(String line) {
        String[] tokens = line.split("\t");
        if (tokens.length >= 3) {
            year = Integer.parseInt(tokens[0]);
            station = tokens[1];
            temperature = Double.parseDouble(tokens[2]);
            valid = true;
        }
    }

    public void parse(Text line) {
        parse(line.toString());
    }

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public String getStation() {
        return station;
    }

    public void setStation(String station) {
        this.station = station;
    }

    public double getTemperature() {
        return temperature;
    }

    public void setTemperature(double temperature) {
        this.temperature = temperature;
    }

    public boolean isValid() {
        return valid;
    }

    public void setValid(boolean valid) {
        this.valid = valid;
    }

}
