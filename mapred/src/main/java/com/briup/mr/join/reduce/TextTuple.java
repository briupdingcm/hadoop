package com.briup.mr.join.reduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TextTuple implements WritableComparable<TextTuple> {
    private Text artistId = new Text();
    private Text tag = new Text();

    @Override
    public void readFields(DataInput in) throws IOException {
        artistId.readFields(in);
        tag.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        artistId.write(out);
        tag.write(out);
    }

    @Override
    public String toString() {
        return artistId + "\t" + tag;
    }

    @Override
    public int compareTo(TextTuple o) {
        int cmp = artistId.compareTo(o.artistId);
        if (cmp != 0)
            return cmp;
        return tag.compareTo(o.tag);
    }

    @Override
    public int hashCode() {
        return Math.abs(artistId.hashCode() * 127 + tag.hashCode());
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof TextTuple) {
            TextTuple tp = (TextTuple) obj;
            return artistId.equals(tp.artistId) && tag.equals(tp.tag);
        }
        return false;
    }

    public Text getArtistId() {
        return artistId;
    }

    public void setArtistId(Text artist) {
        this.artistId.set(artist.toString());
    }

    public Text getIdentity() {
        return tag;
    }

    public void setIdentity(Text identity) {
        this.tag.set(identity.toString());
    }

    public void set(String artist, String tag) {
        artistId.set(artist);
        this.tag.set(tag);
    }
}
