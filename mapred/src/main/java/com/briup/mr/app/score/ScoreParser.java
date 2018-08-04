package com.briup.mr.app.score;

import com.briup.mr.common.Parser;
import org.apache.hadoop.io.Text;

public class ScoreParser implements Parser<String, ScoreParser> {
    private String id;
    private String name;
    private String score;
    private boolean isValid = false;

    public void parse(String line) {
        String[] items = line.split("[|]");
        if (items == null || items.length < 3) {
            isValid = false;
            return;
        }
        id = items[0].trim();
        name = items[1].trim();
        score = items[2].trim();
        isValid = true;

    }

    @Override
    public boolean isValid() {
        return isValid;
    }

    public void parse(Text line) {
        parse(line.toString());
    }

    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public String getScore() {
        return score;
    }

}
