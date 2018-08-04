package com.briup.mr.basic;

import com.briup.mr.common.Parser;
import org.apache.hadoop.io.Text;

public class NcdcRecordParser implements Parser<String, NcdcRecordParser> {
    private static final int MISSING = 9999;
    private String stationId;
    private int year;
    private int temperature;
    private boolean isValidTemperature;

    public NcdcRecordParser() {
    }

    public void parse(String line) {
        if (line.length() < 93) {
            isValidTemperature = false;
            return;
        }
        stationId = line.substring(0, 15);
        year = Integer.parseInt(line.substring(15, 19));
        if (line.charAt(87) == '+') {
            temperature = Integer.parseInt((line.substring(88, 92)));
        } else {
            temperature = Integer.parseInt((line.substring(87, 92)));
        }
        String quality = line.substring(92, 93);
        this.isValidTemperature = (temperature != MISSING) && (quality.matches("[01459]"));
    }

    /*
        public Optional<NcdcRecordParser> parse(Supplier<String> line){
            parse(line);
            if(isValidTemperature())return  Optional.of(this);//this;
            return Optional.empty();
        }
    */
    @Override
    public boolean isValid() {
        return isValidTemperature();
    }

    public String getStationId() {
        return stationId;
    }

    public void setStationId(String stationId) {
        this.stationId = stationId;
    }

    public void parse(Text value) {
        parse(value.toString());
    }

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public int getAirTemperature() {
        return temperature;
    }

    public void setAirTemperature(int temperature) {
        this.temperature = temperature;
    }

    public boolean isValidTemperature() {
        return isValidTemperature;
    }

    public void setValidTemperature(boolean isValidTemperature) {
        this.isValidTemperature = isValidTemperature;
    }
}
