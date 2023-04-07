package com.jaon.mapreducedemo.reduceJoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class TableDriver {
    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException, ClassNotFoundException {
        // 创建job
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        //注册jar
        job.setJarByClass(TableDriver.class);
        //注册map and reduce
        job.setMapperClass(TableMapper.class);
        job.setReducerClass(TableReducer.class);
        // 注册map的输出类
        job.setMapOutputKeyClass(TableBean.class);
        job.setMapOutputValueClass(Text.class);
        // 注册 最终的输出类
        job.setOutputValueClass(TableBean.class);
        job.setOutputKeyClass(Text.class);
        // 下面为job添加需要缓存文件得路径
        job.addCacheFile(new URI("file:///E:/hadoop_data/pname/pd.txt"));
        // 不需要按照文件进行分区，也不需要两个 reduce输出
        job.setNumReduceTasks(1);
        // 输出
        FileInputFormat.addInputPath(job,new Path("E:\\hadoop_data\\order"));
        FileOutputFormat.setOutputPath(job,new Path("E:\\hadoop_data\\order_output\\"+System.currentTimeMillis()));
        //
        boolean r = job.waitForCompletion(true);
        System.exit(r?0:1);

    }
}
