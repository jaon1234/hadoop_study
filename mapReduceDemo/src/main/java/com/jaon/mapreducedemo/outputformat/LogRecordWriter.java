package com.jaon.mapreducedemo.outputformat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class LogRecordWriter extends RecordWriter<Text, NullWritable> {

    private FSDataOutputStream log1Out;
    private FSDataOutputStream log2Out;

    public LogRecordWriter(TaskAttemptContext job){
        try {
            // 获取文件系统对象
            FileSystem fs = FileSystem.get(job.getConfiguration());
            // 用文件系统对象，创建两个输出流对应到不同的目录。
            log1Out = fs.create(new Path("E:\\hadoop_data\\log\\log1.txt"));
            log2Out = fs.create(new Path("E:\\hadoop_data\\log\\log2.txt"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }
    @Override
    public void write(Text key, NullWritable value) throws IOException, InterruptedException {
        String log = key.toString();
        if(log.contains("atguigu")){
            log1Out.writeBytes(log+"\n");
        }else {
            log2Out.writeBytes(log+"\n");
        }
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        IOUtils.closeStream(log1Out);
        IOUtils.closeStream(log2Out);
    }
}
