package com.briup.mr.inputFormat.reader.xml;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

public class XmlReader extends RecordReader<Text, Text> {
    public static final
    String ELEMENT_NAME = "elementName";

    private InputStream fileIn;
    private long start;
    private long end;
    private long current;
    private boolean eof = false;

    private Text key = new Text();
    private StringBuilder value;

    private byte[][] startTag;
    private byte[][] endTag;

    private int[] matchingStartTag;
    private int[] matchingEndTag;

    private byte[] buffer;
    private int bufferPos;
    private int bufferRead;

    public static void main(String[] args) throws IOException {
        args = new String[]{"data.xml", "student", "9", "200"};
        FileInputStream in = new FileInputStream(args[0]);
        long start = Long.parseLong(args[2]);
        long end = Long.parseLong(args[3]);
        XmlReader reader = new XmlReader();
        reader.init(start, end, args[1], in);

        while (reader.nextKeyValue()) {
            System.out.println("key " + reader.getCurrentKey().toString());
            System.out.println("value " + reader.getCurrentValue().toString());
            System.out.println(reader.getProgress());
        }

    }

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException {
        FileSplit split = (FileSplit) inputSplit;
        Configuration configuration =
                context.getConfiguration();
        String key = configuration.get(ELEMENT_NAME);

        long s = split.getStart();
        long e = s + split.getLength() - 1;
        Path file = split.getPath();

        FileSystem fs = file.getFileSystem(configuration);
        FSDataInputStream fileIn = fs.open(file);

        init(s, e, key, fileIn);

    }

    public void init(long start, long end,
                     String key, InputStream in) throws IOException {
        this.start = start;
        this.end = end;
        this.current = this.start;
        this.key = new Text(key);
        this.startTag = new byte[2][];
        this.endTag = new byte[1][];
        // 开始标签的两种形式
        this.startTag[0] = ("<" + key + ">").getBytes();
        this.startTag[1] = ("<" + key + " ").getBytes();
        this.matchingStartTag = new int[2];
        this.matchingEndTag = new int[1];
        fileIn = in;
        //将文件指针指向开始处
        if (this.start > 0) {
            if (fileIn instanceof FSDataInputStream) {
                ((FSDataInputStream) fileIn).seek(this.start);
            } else {
                fileIn.skip(this.start);
            }
        }

        this.buffer = new byte[4096];
        this.bufferPos = -1;
        this.bufferRead = -1;
    }

    public boolean nextKeyValue() {
        this.value = new StringBuilder();
        if ((this.eof) || (this.current >= this.end))
            return false;

        if (!readUntil(this.startTag, this.matchingStartTag, false))// 查找开始标签
            return false;

        if (this.eof) // 如果读到结束位置
            return false;

        String endtag = "</" + key.toString() + ">";

        this.endTag[0] = endtag.getBytes();
        readUntil(this.endTag, this.matchingEndTag, true);// 查找结束标签
        return !this.eof;

    }

    private boolean readUntil(byte[][] match, int[] matchingTag, boolean withinBlock) {
        char currChar = ' ', prevChar = ' ';
        boolean matched = false;
        for (int i = 0; i < matchingTag.length; i++)
            matchingTag[i] = 0;
        for (; ; ) {
            int rv = nextByte();
            this.current++;
            if (rv < 0) { // 读到文件结束
                this.eof = true;
                return false;
            }
            currChar = (char) rv;
            if (withinBlock || matched)
                this.value.append(currChar);
            if (!matched) {
                for (int j = 0; j < matchingTag.length; j++) {
                    if (currChar == match[j][matchingTag[j]]) {//如果当前字符是否匹配标签
                        matchingTag[j]++;
                        if (matchingTag[j] >= match[j].length) {//如果匹配上
                            matched = true;
                            if (withinBlock) return false;
                            long st = this.current - match[j].length;
                            if (st >= this.end) { // 读到的块的结束位置
                                this.eof = true;
                                return false;
                            }
                            this.value.append(new String(match[j]));
                            //		if (j >= 1)j -= 1;
                            if ('>' == this.value.charAt(this.value.length() - 1))
                                return false;
                            break;
                        }
                    } else  //如果当前字符是否匹配标签
                        matchingTag[j] = 0;
                }
                prevChar = currChar;
                continue;
            }

            // We are matched, go till the end of the tag
            if (currChar == '>') {
                return !withinBlock && (prevChar == '/');
            }
            prevChar = currChar;

        }

    }

    private int nextByte() {
        if (this.bufferRead == -1) {
            try {
                this.bufferRead = fileIn.read(this.buffer);
                if (this.bufferRead < 0) { //读到文件结束
                    return -1;
                }
            } catch (Exception e) { //读到文件出错（视为结束）
                this.eof = true;
                return -1;
            }
            this.bufferPos = 0;
        }
        int value = 0x00ff & this.buffer[this.bufferPos++];
        if (this.bufferPos >= this.bufferRead) {
            this.bufferRead = -1;
        }
        return value;
    }

    @Override
    public Text getCurrentKey() {
        return key;
    }

    @Override
    public Text getCurrentValue() {
        return new Text(value.toString());
    }

    public float getProgress() {
        if (this.start == this.end)
            return 0.0f;
        else
            return Math.min(1.0f, (this.current - this.start) / (float) (this.end - this.start));
    }

    public void close() throws IOException {
        if (fileIn != null)
            fileIn.close();

    }
}
