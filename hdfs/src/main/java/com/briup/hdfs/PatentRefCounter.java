package com.briup.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

/**
 * hadoop jar target/hdfs-0.0.1.jar com.briup.hdfs.PatentRefCounter -D
 * input=/data/patent/cite75_99.txt -D output=./patentRef.txt
 *
 * @author kevin
 */
public class PatentRefCounter extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new PatentRefCounter(), args));
    }

    public int run(String[] arg0) throws Exception {
        Configuration conf = getConf();
        Path input = new Path(conf.get("input"));
        Path output = new Path(conf.get("output"));

        FileSystem fs = FileSystem.get(conf);
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(input)));
        PrintWriter pw = new PrintWriter(fs.create(output));

        Map<String, Long> counter = br.lines().filter(line -> line != null)
                .map(x -> x.split(",")).filter(x -> x != null && x.length >= 2)
                .reduce(new HashMap<String, Long>(),
                        (m, e) -> {
                            m.put(e[1], m.getOrDefault(e[1], 0l) + 1);
                            return m;
                        },
                        (l, r) -> {
                            r.forEach((k, v) -> {
                                l.put(k, l.getOrDefault(k, 0l) + v);
                            });
                            return l;
                        });
        counter.forEach((k, v) -> pw.printf("%s, %s\n", k, v));
        pw.close();
        return 0;
    }
}
