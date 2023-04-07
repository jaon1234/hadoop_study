package com.jaon.mapreducedemo.writableComparablePartition;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FlowReducer extends Reducer<FlowBean,Text,Text,FlowBean> {

    @Override
    protected void reduce(FlowBean  key, Iterable<Text> values,Context context)
            throws IOException, InterruptedException {
                // 希望是输出手机号、上行流量、下行流量、总流量的形式
                // 遍历value 获取手机号
                for(Text value:values){
                    // 写出手机号和对应的FlowBean
                    context.write(value, key);
                }
    }
}
