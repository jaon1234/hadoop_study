package com.jaon.mapreducedemo.writableComparablePartition;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class ProvincePartition extends Partitioner<FlowBean, Text> {

    @Override
    public int getPartition(FlowBean key, Text value, int numPartitions) {

        int part = 0;
        String province = value.toString().substring(0, 3);
        if ("134".equals(province)) {
            part = 0;
        } else if ("135".equals(province)) {
            part = 1;
        } else if ("136".equals(province)) {
            part = 2;
        } else if ("137".equals(province)) {
            part = 3;
        } else {
            part = 4;
        }
        return part;
    }
}
