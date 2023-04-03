package com.jaon.mapreducedemo.writable;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FlowReducer extends Reducer<Text,FlowBean,Text,FlowBean> {
    private FlowBean flowBean = new FlowBean();
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Reducer<Text, FlowBean, Text, FlowBean>.Context context)
            throws IOException, InterruptedException {
                // 获取到的数据是key和一堆flowbean
                long upFlow  = 0;
                long downFlow = 0;
                for(FlowBean v:values){
                    upFlow += v.getUpFlow();
                    downFlow += v.getDownFlow();
                }
                // 给bean赋值
                flowBean.setUpFlow(upFlow);
                flowBean.setDownFlow(downFlow);
                flowBean.setSumFlow();

                // 输出数据
                context.write(key, flowBean);
    }
}
