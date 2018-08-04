package com.briup.mr.join.reduce;

import com.briup.mr.common.Parser;
import org.apache.hadoop.io.Text;

public class ArtistMetaParser implements Parser<String, ArtistMetaParser> {
    private String artistId;
    private String artistName;
    private String date;
    private boolean valid = false;

    public void parse(String line) {
        String[] tokens = line.split(",");
        if (tokens != null & tokens.length == 3) {
            artistId = tokens[0].trim();
            artistName = tokens[1].trim();
            date = tokens[2].trim();
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

    public String getArtistName() {
        return artistName;
    }

    public void setArtistName(String artistName) {
        this.artistName = artistName;
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
