package com.jaon.mapreducedemo.writable;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FlowDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 1、创建job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 2、声明jar
        job.setJarByClass(FlowDriver.class);

        // 3、关联mapper和reducer
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);

        //4、设置mapper的输出
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        //5、设置最终的输出
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        //6、设置输入输出路径
        FileInputFormat.setInputPaths(job, "E:\\hadoop_data\\phoneInput\\");
        FileOutputFormat.setOutputPath(job, new Path("E:\\hadoop_data\\phoneoutput\\"));

        //7、提交job
        boolean result = job.waitForCompletion(true);
        System.exit(result? 0:1);
    }
}
