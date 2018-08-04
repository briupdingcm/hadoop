package com.briup.mr;

import com.briup.hdfs.HdfsTools;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.*;


public class kNN extends Configured implements Tool {

    public static void main(String[] args) throws Exception {

        String src = "hdfs://master:9000/MachineLearning/kNN/kNNdata-src.txt";
        String input = "hdfs://master:9000/MachineLearning/kNN/kNNdata-data.txt";
        String output = "hdfs://master:9000/MachineLearning/kNN/result";

        int kNN_k = 10;
        //分隔符，切割后的总列数，key所在的列，value所在的列，计算使用的列，计算使用的列，计算使用的列,......
        String src_column = "\t,4,0,3,1,2";
        String data_column = "\t,4,0,3,1,2";

        HdfsTools.delete(output);
        if (args.length == 0) {
            args = new String[]{"-Dinput=" + input, "-Doutput=" + output, "-Dsrc_path=" + src, "-DkNN_k=" + kNN_k, "-Dsrc_column=" + src_column, "-Ddata_column=" + data_column};
        } else if (args.length != 6) {
            System.out.println("缺少参数！所需参数如下：");
            System.out.println("-Dinput\t待分类的数据文件，如：-Dinput=hdfs://master:9000/MachineLearning/kNN/kNNdata-src.txt");
            System.out.println("-Doutput\t输出路径");
            System.out.println("-Dsrc_path\t已分类的数据文件，如：-Dinput=hdfs://master:9000/MachineLearning/kNN/kNNdata-data.txt");
            System.out.println("-DkNN_k\tkNN算法的k值");
            System.out.println("-Dsrc_column\t值的格式：[分隔符，切割后的总列数，key所在的列，value所在的列，计算使用的列，计算使用的列，计算使用的列,......], 如：-Dsrc_column=\\t,4,0,3,1,2");
            System.out.println("-Ddata_column\t见-Dsrc_column");
        }

        int status = ToolRunner.run(new kNN(), args);
        if (status == 0) {
            HdfsTools.cat(output + "/part-r-00000");
        }
        System.exit(status);
    }

    @Override
    public int run(String[] arg0) throws Exception {
        // 获得程序运行时的配置信息
        Configuration conf = this.getConf();
        String inputPath = conf.get("input");
        String outputPath = conf.get("output");
        // 构建新的作业
        Job job = Job.getInstance(conf, "chrasm_kNN");
        job.setJarByClass(kNN.class);
        // 给job设置mapper类及map方法输出的键值类型
        job.setMapperClass(myMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        // 给job设置reducer类及reduce方法输出的键值类型
        job.setReducerClass(myReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        // 设置数据的读取方式（文本文件）及结果的输出方式（文本文件）
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        // 设置输入和输出目录
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        // 将作业提交集群执行
        return job.waitForCompletion(true) ? 0 : 1;
//        int status = job.waitForCompletion(true) ? 0 : 1;
//        if(status==0){
//            HdfsTools.cat(outputPath+"/part-r-00000");
//        }
//        return status;
    }

    static class myMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
        private String src;
        private int kNN_k = 10;
        private String src_separator;
        private String data_separator;
        private int[] src_item;
        private int[] data_item;

        private FileSystem fileSystem;
        private NullWritable v = NullWritable.get();

        @Override
        protected void setup(Context context) throws IOException {
            kNN_k = Integer.parseInt(context.getConfiguration().get("kNN_k", "10"));
            src = context.getConfiguration().get("src_path");
            String[] src_columns = context.getConfiguration().get("src_column").split("[,]");
            String[] data_columns = context.getConfiguration().get("data_column").split("[,]");
            src_separator = src_columns[0];
            data_separator = data_columns[0];

            src_item = new int[src_columns.length - 1];
            data_item = new int[data_columns.length - 1];

            for (int i = 0; i < src_item.length; i++) {
                src_item[i] = Integer.parseInt(src_columns[i + 1]);
                data_item[i] = Integer.parseInt(data_columns[i + 1]);
            }

            fileSystem = FileSystem.get(URI.create(src), new Configuration());
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            TreeMap<Double, String> dists = new TreeMap<>(new Comparator<Double>() {
                @Override
                public int compare(Double o1, Double o2) {
                    return o1 - o2 == 0 ? 1 : o1 - o2 > 0 ? 1 : -1;
                }
            });
            String[] datas = value.toString().split("[" + data_separator + "]");

            if (datas.length != data_item[0]) return;

            String k = datas[data_item[1]];
            double[] x = new double[data_item.length - 3];
            for (int i = 0; i < x.length; i++) {
                x[i] = Double.parseDouble(datas[data_item[i + 3]]);
            }
            BufferedReader br = new BufferedReader(new InputStreamReader(fileSystem.open(new Path(src))));

            String line = "";
            while ((line = br.readLine()) != null) {
                String[] srcs = line.split("[" + src_separator + "]");
                if (srcs.length != src_item[0]) continue;

                double[] y = new double[src_item.length - 3];
                for (int i = 0; i < y.length; i++) {
                    y[i] = Double.parseDouble(srcs[src_item[i + 3]]);
                }
                double dist = 0;
                for (int i = 0; i < x.length; i++) {
                    dist += Math.pow(x[i] - y[i], 2);
                }

                dists.put(Math.sqrt(dist), srcs[src_item[2]]);
                if (dists.size() >= kNN_k) dists.pollLastEntry();
            }
            br.close();

            Map<String, Double[]> map = new HashMap<>();
            int id = 1;
            Iterator<Map.Entry<Double, String>> iterator = dists.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<Double, String> next = iterator.next();
                double v = (1.0 / (next.getKey() + 10)) * (1.0 / (id + 20));
                if (map.containsKey(next.getValue())) {
                    Double[] doubles = map.get(next.getValue());
                    doubles[0] += v;
                    doubles[1] += 1;
                } else map.put(next.getValue(), new Double[]{v, 1.0});
                id++;
            }
            TreeMap<Double, String> result = new TreeMap(new Comparator<Double>() {
                @Override
                public int compare(Double o1, Double o2) {
                    return o1 - o2 == 0 ? 1 : o1 - o2 > 0 ? -1 : 1;
                }
            });
            Iterator<Map.Entry<String, Double[]>> iterator1 = map.entrySet().iterator();

            while (iterator1.hasNext()) {
                Map.Entry<String, Double[]> next = iterator1.next();
                result.put(next.getValue()[0] / next.getValue()[1], next.getKey());
            }
            String vstr = k + "\t";
            for (int i = 3; i < data_item.length; i++) {
                vstr = vstr + datas[data_item[i]] + "\t";
            }


            Map.Entry<Double, String> doubleStringEntry = result.pollFirstEntry();
            double sm = doubleStringEntry.getKey();
            vstr = vstr + doubleStringEntry.getValue();
            Iterator<Map.Entry<Double, String>> iterator2 = result.entrySet().iterator();
            while (iterator2.hasNext()) {
                Map.Entry<Double, String> next = iterator2.next();
                if (next.getKey() == sm) {
                    vstr = vstr + "," + next.getValue();
                } else break;
            }

            context.write(new Text(vstr + " : [类型相似度平均值：" + sm + "]"), v);
        }

//        @Override
//        protected void cleanup(Context context) throws IOException, InterruptedException {
//            fileSystem.close();
//        }
    }

    static class myReducer extends Reducer<Text, NullWritable, Text, NullWritable> {

        private NullWritable v = NullWritable.get();

        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key, v);
        }
    }

}
