package com.jaon.mapreducedemo.reduceJoin;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TableReducer extends Reducer<TableBean,Text,Text,TableBean> {
    @Override
    protected void reduce(TableBean key, Iterable<Text> values, Reducer<TableBean,Text,Text,TableBean>.Context context) throws IOException, InterruptedException {
        for(Text value:values){
            context.write(value,key);
        }
    }
}
