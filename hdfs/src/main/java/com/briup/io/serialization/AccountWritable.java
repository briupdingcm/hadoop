package com.briup.io.serialization;

import org.apache.hadoop.io.*;

import java.io.*;

public class AccountWritable implements WritableComparable<AccountWritable> {
    private IntWritable code;
    private Text name;
    private BooleanWritable gender;

    public AccountWritable() {
        code = new IntWritable();
        name = new Text();
        gender = new BooleanWritable();
    }

    /*
     * 复制构造器一定要将另一个同类型对象的属性做整体复制， 而不能是引用复制。
     */
    public AccountWritable(AccountWritable other) {
        code = new IntWritable(other.code.get());
        name = new Text(other.name);
        gender = new BooleanWritable(other.gender.get());
    }

    public static void main(String... strings) {
        AccountWritable aw1 = new AccountWritable();
        aw1.set(new IntWritable(30), new Text("kevin"), new BooleanWritable(true));

        AccountWritable aw2 = new AccountWritable();
        aw2.set(new IntWritable(30), new Text("kevin"), new BooleanWritable(true));

        AccountWritable.Comparator cmp = new AccountWritable.Comparator();
        System.out.println(cmp.compare(aw1, aw2));
    }

    @Override
    public void write(DataOutput out) throws IOException {
        code.write(out);
        name.write(out);
        gender.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        code.readFields(in);
        name.readFields(in);
        gender.readFields(in);
    }

    /* 属性复制而不是引用复制 */
    public void set(IntWritable code, Text name, BooleanWritable gender) {
        this.code = new IntWritable(code.get());
        this.name = new Text(name);
        this.gender = new BooleanWritable(gender.get());
    }

    @Override
    public int compareTo(AccountWritable o) {
        return this.code.compareTo(o.code) != 0 ? code.compareTo(o.code)
                : (name.compareTo(o.name) != 0 ? name.compareTo(o.name)
                : (gender.compareTo(o.gender)));
    }

    public static class Comparator implements RawComparator<AccountWritable> {

        private final IntWritable.Comparator IC = new IntWritable.Comparator();
        private final Text.Comparator TC = new Text.Comparator();
        private final BooleanWritable.Comparator BC = new BooleanWritable.Comparator();

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            // code被序列化后在b1或b2数组中的起始位置和字节长度
            int firstLen = 4, secondLen = 4;
            int firstStart = s1, secondStart = s2;
            int firstOffset = 0, secondOffset = 0;
            // 比较字节流中的code部分
            int cmp = IC.compare(b1, firstStart, firstLen, b2, secondStart, secondLen);
            if (cmp != 0)
                return cmp;
            else {
                // name被序列化后在b1或b2数组中的起始位置和字节长度
                firstStart = firstStart + firstLen;
                secondStart = secondStart + secondLen;
                try {
                    // 字符串长度值(变长整数)所占字节数
                    firstOffset = WritableUtils.decodeVIntSize(b1[firstStart]);
                    secondOffset = WritableUtils.decodeVIntSize(b2[secondStart]);
                    // 字符串长度
                    firstLen = readLengthValue(b1, firstStart);
                    secondLen = readLengthValue(b2, secondStart);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                // 比较字节流中的name部分
                cmp = TC.compare(b1, firstStart + firstOffset, firstLen, b2, secondStart + secondOffset, secondLen);
                if (cmp != 0)
                    return cmp;
                else {
                    // gender被序列化后在b1或b2数组中的起始位置和字节长度
                    firstStart = firstStart + firstOffset + firstLen;
                    secondStart = secondStart + secondOffset + secondLen;
                    firstLen = 1;
                    secondLen = 1;
                    // 比较字节流中的gender部分
                    return BC.compare(b1, firstStart, firstLen, b2, secondStart, secondLen);
                }
            }
        }

        private int readLengthValue(byte[] bytes, int start) throws IOException {
            DataInputStream dis = new DataInputStream(
                    new ByteArrayInputStream(bytes, start, WritableUtils.decodeVIntSize(bytes[start])));
            VIntWritable viw = new VIntWritable();
            viw.readFields(dis);
            return viw.get();
        }

        @Override
        public int compare(AccountWritable o1, AccountWritable o2) {
            ByteArrayOutputStream baos1 = new ByteArrayOutputStream();
            DataOutputStream dos1 = new DataOutputStream(baos1);

            ByteArrayOutputStream baos2 = new ByteArrayOutputStream();
            DataOutputStream dos2 = new DataOutputStream(baos2);

            try {
                o1.write(dos1);
                o2.write(dos2);
                dos1.close();
                dos2.close();
                byte[] b1 = baos1.toByteArray();
                byte[] b2 = baos2.toByteArray();
                return compare(b1, 0, b1.length, b2, 0, b2.length);
            } catch (IOException e) {
                e.printStackTrace();
            }
            return 0;
        }
    }
}
