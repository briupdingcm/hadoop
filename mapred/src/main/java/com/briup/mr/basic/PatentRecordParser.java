package com.briup.mr.basic;

import com.briup.mr.common.Parser;
import org.apache.hadoop.io.Text;

public class PatentRecordParser implements Parser<String, PatentRecordParser> {
    private String patentId;
    private String refPatentId;
    private boolean isValid = false;

    public void parse(String line) {
        String[] ps = line.split(",");
        if (ps.length >= 2) {
            patentId = ps[0].trim();
            refPatentId = ps[1].trim();
        }
        if (patentId.length() > 0 && refPatentId.length() > 0)
            isValid = true;
    }

    public void parse(Text line) {
        parse(line.toString());
    }

    public String getPatentId() {
        return patentId;
    }

    public void setPatentId(String patentId) {
        this.patentId = patentId;
    }

    public String getRefPatentId() {
        return refPatentId;
    }

    public void setRefPatentId(String refPatentId) {
        this.refPatentId = refPatentId;
    }

    public boolean isValid() {
        return isValid;
    }

    public void setValid(boolean isValid) {
        this.isValid = isValid;
    }


}
