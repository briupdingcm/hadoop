package com.briup.mr.inputFormat.split;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

public class FileListInputFormat extends FileInputFormat<Text, BytesWritable> {
    private static final String MAPCOUNT = "mapreduce.job.maps";

    public static void setMapCount(Job job, int mappers) {
        job.getConfiguration().set(MAPCOUNT, new Integer(mappers).toString());
    }

    @Override
    public List<InputSplit> getSplits(JobContext job) throws IOException {
        Configuration conf = job.getConfiguration();
        // 获得数据所在的服务器地址
        String[] hosts = getActiveServersList(job);
        List<InputSplit> splits = new ArrayList<InputSplit>();

        int mappers = 0;

        mappers = Integer.parseInt(conf.get(MAPCOUNT) == null ? "1" : conf.get(MAPCOUNT));
        if (mappers == 0)
            throw new IOException("Number of mappers is not specified");
        // 获得所有文件元数据
        List<FileStatus> files = listStatus(job);
        int nfiles = files.size();
        if (nfiles < mappers)
            mappers = nfiles;
        // 构建所有的数据分片
        for (int i = 0; i < mappers; i++)
            splits.add(new MultiFileSplit(0, hosts));
        // 将文件路径分别加入数据分片
        Iterator<InputSplit> siter = splits.iterator();
        for (FileStatus f : files) {
            if (!siter.hasNext())
                siter = splits.iterator();
            String sp = f.getPath().toUri().getPath();
            ((MultiFileSplit) (siter.next())).addFile(sp);
        }
        int max = Integer.MIN_VALUE;
        siter = splits.iterator();
        int counter = 0;
        while (siter.hasNext()) {
            MultiFileSplit mfs = (MultiFileSplit) siter.next();
            max = mfs.getMaxLocation() < max ? max : mfs.getMaxLocation();
            counter += mfs.getMaxLocation();
        }
        job.getConfiguration().setInt("mapreduce.job.max.split.locations", max);
        return splits;
    }

    private String[] getActiveServersList(JobContext context) {
        String[] servers = null;
        int s = 0;

        try {
            Cluster cluster = new Cluster(context.getConfiguration());
            TaskTrackerInfo[] ttis = cluster.getActiveTaskTrackers();
            if (ttis.length == 0) {
                servers = new String[]{"localhost"};
            } else {
                servers = new String[ttis.length];
                for (TaskTrackerInfo tti : ttis) {
                    StringTokenizer st = new StringTokenizer(tti.getTaskTrackerName(), ":");
                    String trackerName = st.nextToken();
                    StringTokenizer st1 = new StringTokenizer(trackerName, "_");
                    st1.nextToken();
                    servers[s] = st1.nextToken();
                    s++;
                }
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
        return servers;
    }

    @Override
    public RecordReader<Text, BytesWritable> createRecordReader(InputSplit split, TaskAttemptContext context) {
        FileRecordReader lrr = new FileRecordReader();
        lrr.initialize(split, context);
        return lrr;
    }

}
