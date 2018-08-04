package com.briup.mr.join.reduce;

import com.briup.mr.common.Parser;
import org.apache.hadoop.io.Text;

public class ArtistRecorderParser implements Parser<String, ArtistRecorderParser> {
    private String artistId;
    private String date;
    private int counter;
    private boolean valid = false;

    public void parse(String line) {
        String[] tokens = line.split(",");
        if (tokens != null & tokens.length == 3) {
            artistId = tokens[0].trim();
            date = tokens[1].trim();
            counter = Integer.parseInt(tokens[2].trim());
            valid = true;
        }
    }

    public void parse(Text line) {
        parse(line.toString());
    }

    public String getArtistId() {
        return artistId;
    }

    public void setArtistId(String artistId) {
        this.artistId = artistId;
    }

    public int getCounter() {
        return counter;
    }

    public void setCounter(int counter) {
        this.counter = counter;
    }

    public boolean isValid() {
        return valid;
    }

    public void setValid(boolean valid) {
        this.valid = valid;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }


}
