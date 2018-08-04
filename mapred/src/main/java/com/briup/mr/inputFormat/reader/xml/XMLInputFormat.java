package com.briup.mr.inputFormat.reader.xml;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

public class XMLInputFormat
        extends FileInputFormat<Text, Text> {

    public static void setElementName(
            Job job, String elementName) {
        job.getConfiguration().set(
                XmlReader.ELEMENT_NAME, elementName);
    }

    @Override
    public RecordReader<Text, Text> createRecordReader(
            InputSplit InputSplit, TaskAttemptContext context) throws IOException,
            InterruptedException {
        RecordReader<Text, Text> rr = new XmlReader();
        rr.initialize(InputSplit, context);
        return rr;
    }
}






