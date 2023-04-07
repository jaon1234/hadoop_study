package com.jaon.mapreducedemo.outputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class LogDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // 创建job
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        // 注册job的jar
        job.setJarByClass(LogDriver.class);

        //注册map和reduce的jar
        job.setMapperClass(LogMapper.class);
        job.setReducerClass(LogReducer.class);

    //    设定map输入输出的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

    //    设定最终的输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        // 设定自定义的outputformat
        job.setOutputFormatClass(LogOutFormat.class);

        FileInputFormat.setInputPaths(job,"E:\\hadoop_data\\loginput");
        FileOutputFormat.setOutputPath(job,new Path("E:\\hadoop_data\\logoutput"));

        boolean r = job.waitForCompletion(true);
        System.exit(r?0:1);


    }
}
