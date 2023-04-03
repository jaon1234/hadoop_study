package com.jaon.mapreducedemo.provincePartition;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class ProvincePartition extends Partitioner<Text,FlowBean> {
    @Override
    public int getPartition(Text key, FlowBean value, int numPartitions) {
        int part ;
        String phone = key.toString();
        String province = phone.substring(0,3);
        if("134".equals(province)){
            part = 0;
        }else if("135".equals(province)){
            part = 1;
        }else if("136".equals(province)){
            part = 2;
        }else if("137".equals(province)){
            part = 3;
        }else{
            part = 4;
        }
        return part;
    }
    
}
