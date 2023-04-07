package com.jaon.mapreducedemo.writableComparablePartition;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FlowMapper extends Mapper<LongWritable,Text,FlowBean,Text> {
    private FlowBean flowBean = new FlowBean();
    private Text outValue = new Text();
    
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
                // 获取一行
                String line = value.toString();
                // 对这一行进行切分
                String[] split = line.split("\t");
                // 读取需要的数据，构造key和value
                String phone = split[0];
                String upFlow = split[split.length - 3];
                String downFlow = split[split.length - 2];

                // 封装k、v
                outValue.set(phone);
                flowBean.setUpFlow(Long.parseLong(upFlow));
                flowBean.setDownFlow(Long.parseLong(downFlow));
                flowBean.setSumFlow();

                // 输出key和value
                context.write(flowBean, outValue);

    }
}
