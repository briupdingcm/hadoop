package com.briup.rmc.common;

import com.briup.rmc.coMatrix.MatrixVectorMultiple.PreferenceOrCooccurence;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

public class Heartbeat extends Thread {
    private static final int sleepTime = 400;
    private boolean beating = true;
    private TaskInputOutputContext<PreferenceOrCooccurence, VectorWritable, LongWritable, Preference> context = null;

    private Heartbeat(TaskInputOutputContext<PreferenceOrCooccurence, VectorWritable, LongWritable, Preference> context) {
        this.context = context;
    }

    public static Heartbeat createHeartbeat(TaskInputOutputContext<PreferenceOrCooccurence, VectorWritable, LongWritable, Preference> context) {
        Heartbeat heartbeat = new Heartbeat(context);
        Thread hbt = new Thread(heartbeat);
        hbt.setPriority(MAX_PRIORITY);
        hbt.setDaemon(true);
        hbt.start();
        return heartbeat;
    }

    @Override
    public void run() {
        while (beating) {
            try {
                sleep(sleepTime * 1000);
            } catch (InterruptedException e) {

            }
            context.setStatus(Long.valueOf(System.currentTimeMillis()).toString());
        }
    }

    public void stopbeating() {
        beating = false;
    }
}
