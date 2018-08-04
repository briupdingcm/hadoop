package com.briup.io.serialization;

import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.Writable;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class VIntWritableTest {
    public static byte[] serialize(Writable writable) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputStream dataOut = new DataOutputStream(out);
        writable.write(dataOut);
        dataOut.close();
        return out.toByteArray();
    }

    public static void main(String[] args) throws IOException {
        VIntWritable viw = new VIntWritable(8);
        byte[] outArray = serialize(viw);
        System.out.println(outArray.length);

        System.out.println("-----------");
        viw.set(65535);
        outArray = serialize(viw);
        System.out.println(outArray.length);
    }

}
