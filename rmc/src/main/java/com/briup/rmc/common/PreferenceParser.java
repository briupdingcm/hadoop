package com.briup.rmc.common;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class PreferenceParser {
    private LongWritable itemId = new LongWritable();
    private Preference preference = new Preference();

    public boolean parse(String line) {
        String[] tokens = line.split("\t");
        if (tokens != null && tokens.length >= 2) {
            itemId.set(Long.parseLong(tokens[0].trim()));
            String[] prefs = tokens[1].trim().split(":");
            preference.set(
                    Long.parseLong(prefs[0].trim()),
                    Double.parseDouble(prefs[1].trim())
            );
            return true;
        }
        return false;
    }

    public boolean parse(Text line) {
        return parse(line.toString());
    }

    public LongWritable getItemId() {
        return itemId;
    }

    public void setItemId(LongWritable itemId) {
        this.itemId = itemId;
    }

    public Preference getPreference() {
        return preference;
    }

    public void setPreference(Preference preference) {
        this.preference = preference;
    }

}
