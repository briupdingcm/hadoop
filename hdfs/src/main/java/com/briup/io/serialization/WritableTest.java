package com.briup.io.serialization;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.*;

public class WritableTest {
    public static byte[] serialize(Writable writable) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputStream dataOut = new DataOutputStream(out);
        writable.write(dataOut);
        dataOut.close();
        return out.toByteArray();
    }

    public static byte[] serialize(Integer writable) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputStream dataOut = new DataOutputStream(out);
        ObjectOutputStream oos = new ObjectOutputStream(dataOut);
        oos.writeObject(writable);
        oos.close();
        return out.toByteArray();
    }

    public static byte[] deserialize(
            Writable writable, byte[] bytes) throws IOException {
        ByteArrayInputStream in =
                new ByteArrayInputStream(bytes);
        DataInputStream dataIn =
                new DataInputStream(in);
        writable.readFields(dataIn);
        dataIn.close();
        return bytes;
    }

    public static void main(String[] args) throws IOException {
        IntWritable iw = new IntWritable(3);
        iw.set(3);
        byte[] outArray = serialize(iw);
        System.out.println(outArray.length);

        Integer io = new Integer(3);
        outArray = serialize(io);
        System.out.println(outArray.length);


    }

}
