package com.jaon.mapreducedemo.wordcountCombine;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 1、创建job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 2、注册driver的驱动
        job.setJarByClass(WordCountDriver.class);

        // 3、关联Mapper 和 Reducer的jar
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        // 4、设置mapper的输入数据类型
        job.setMapOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 5、设置最终的输出类型，这里就是reducer的输出
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 设置输入数据处理的方式，如果不设置，默认使用TextInputFormt.class，按行切分数据
        job.setInputFormatClass(CombineTextInputFormat.class);
        //设置虚拟存储切片的最大值
        CombineTextInputFormat.setMaxInputSplitSize(job, 20971520);

        // 6、设置输入和输出路径
        FileInputFormat.setInputPaths(job, "E:\\hadoop_data\\wc_combine\\");
        FileOutputFormat.setOutputPath(job, new Path("E:\\hadoop_data\\wc_combine_out03\\"));

        // 7、提交job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
