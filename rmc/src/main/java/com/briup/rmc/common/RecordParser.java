package com.briup.rmc.common;

import org.apache.hadoop.io.Text;

public class RecordParser {
    private long userId;
    private long itemId;
    private double preference;

    public boolean parse(String line) {
        String[] tokens = line.split(" ");
        if (tokens != null && tokens.length >= 3) {
            userId = Long.parseLong(tokens[0].trim());
            itemId = Long.parseLong(tokens[1].trim());
            preference = Double.parseDouble(tokens[2].trim());
            return true;
        }
        return false;
    }

    public boolean parse(Text line) {
        return parse(line.toString());
    }

    public long getUserId() {
        return userId;
    }

    public void setUserId(long userId) {
        this.userId = userId;
    }

    public long getItemId() {
        return itemId;
    }

    public void setItemId(long itemId) {
        this.itemId = itemId;
    }

    public double getPreference() {
        return preference;
    }

    public void setPreference(double preference) {
        this.preference = preference;
    }

}
