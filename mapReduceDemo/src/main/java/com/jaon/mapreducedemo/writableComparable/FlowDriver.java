package com.jaon.mapreducedemo.writableComparable;

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
        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);

        //5、设置最终的输出
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        //6、设置输入输出路径
        // 这里的format其实声明了数据的读取方式，按行读取
        FileInputFormat.setInputPaths(job, "E:\\hadoop_data\\phoneoutput\\");
        // 这里其是声明了数据的输出方式，按行输出到文件
        FileOutputFormat.setOutputPath(job, new Path("E:\\hadoop_data\\phoneoutputWritableComparable01\\"));

        //7、提交job
        boolean result = job.waitForCompletion(true);
        System.exit(result? 0:1);
    }
}
