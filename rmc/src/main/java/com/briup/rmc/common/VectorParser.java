package com.briup.rmc.common;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.StringTokenizer;

public class VectorParser {
    private LongWritable itemId = new LongWritable();
    private VectorWritable userVec = new VectorWritable();

    public static void main(String... strings) throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://172.16.0.101:9000/");
        FileSystem fs = FileSystem.get(conf);
        BufferedReader br = new BufferedReader(new InputStreamReader(
                fs.open(new Path("/user/dingcm/rec/user/part-r-00000"))));
        String line = null;
        VectorParser uvp = new VectorParser();
        while ((line = br.readLine()) != null) {
            if (uvp.parse(line)) {
                System.out.printf("%s, %s\n", uvp.getItemId(), uvp.getUserVec());
            }
        }
        fs.close();
    }

    public boolean parse(String line) {
        String[] tokens = line.split("\t");
        if (tokens != null && tokens.length >= 2) {
            itemId.set(Long.parseLong(tokens[0].trim()));
            String pref = tokens[1].trim().substring(1, tokens[1].trim().length() - 1);
            StringTokenizer st = new StringTokenizer(pref, ",");
            while (st.hasMoreTokens()) {
                String[] ttt = st.nextToken().trim().split(":");
                userVec.add(new Preference(
                        Long.parseLong(ttt[0].trim()),
                        Double.parseDouble(ttt[1].trim())
                ));
            }
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

    public VectorWritable getUserVec() {
        return userVec;
    }

    public void setUserVec(VectorWritable userVec) {
        this.userVec = userVec;
    }
}
