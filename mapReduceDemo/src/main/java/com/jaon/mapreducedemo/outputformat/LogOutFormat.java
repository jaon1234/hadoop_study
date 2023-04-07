package com.jaon.mapreducedemo.outputformat;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class LogOutFormat extends FileOutputFormat<Text, NullWritable> {
    @Override
    public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
        // 是一次写入获取一个？还是一个map获取一个？一个map获取一个？还是至始至终只有一个？
        LogRecordWriter logRecordWriter = new  LogRecordWriter(job);
        return logRecordWriter;
    }
}
