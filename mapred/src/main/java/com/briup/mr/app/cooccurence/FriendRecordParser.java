package com.briup.mr.app.cooccurence;

import com.briup.mr.common.Parser;
import org.apache.hadoop.io.Text;

public class FriendRecordParser implements Parser<String, FriendRecordParser> {
    private String userName;
    private String friendName;
    private boolean isValid = false;

    public void parse(String line) {
        String[] ps = line.split(",");
        if (ps != null && ps.length >= 2) {
            userName = ps[0].trim();
            friendName = ps[1].trim();
        }
        if (userName.length() > 0
                && friendName.length() > 0)
            isValid = true;
    }

    public void parse(Text line) {
        parse(line.toString());
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getFriendName() {
        return friendName;
    }

    public void setFriendName(String friendName) {
        this.friendName = friendName;
    }

    public boolean isValid() {
        return isValid;
    }

    public void setValid(boolean isValid) {
        this.isValid = isValid;
    }

}
