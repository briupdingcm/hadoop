package com.briup.io.serialization;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;

import java.io.*;

public class TextLengthTest {
    public static void main(String... strings) throws IOException {
        StringBuffer sb = new StringBuffer();
        for (int i = 1; i <= 255; i++)
            sb.append("k");

        System.out.println(sb.toString().length());

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        Text t = new Text(sb.toString());
        t.write(dos);
        dos.close();
        byte[] bytes = baos.toByteArray();
        System.out.println(bytes.length);

        ByteArrayInputStream bais = new ByteArrayInputStream(bytes, 0, bytes.length);
        DataInputStream dis = new DataInputStream(bais);
        t.readFields(dis);
        System.out.println(t);

        int offset = WritableUtils.decodeVIntSize(bytes[0]);
        int length = bytes.length - offset;
        System.out.println(new String(bytes, offset, length));
    }
}
