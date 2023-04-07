package com.jaon.mapreducedemo.reduceJoin;

import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Map;

public class TableMapper extends Mapper<LongWritable, Text,TableBean, Text> {

    Map<String,String> pNames = new  HashedMap();
    private TableBean tableBean = new TableBean();
    private Text out_k = new Text();
    // 读取商品表的缓存文件
    @Override
    protected void setup(Mapper<LongWritable, Text, TableBean, Text>.Context context) throws IOException, InterruptedException {
        // 获取缓存文件名称及路径
        URI[] uris = context.getCacheFiles();
        Path path = new Path(uris[0]);
        // 读取缓存文件，分别得到pid和商品名称，存储在字典当中
        FileSystem fs = FileSystem.get(context.getConfiguration());
        FSDataInputStream inStream = fs.open(path);
        BufferedReader data=new BufferedReader(new InputStreamReader(inStream));//防止中文乱码
        // 逐行读取，并且存储在字典中
        String line;
        line = data.readLine();
        while (StringUtils.isNotEmpty(line)){
            String[] arr = line.split("\t");
            pNames.put(arr[0],arr[1]);
            line = data.readLine();
        }
        data.close();
    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, TableBean, Text>.Context context) throws IOException, InterruptedException {
        // 按行读取
        String line = value.toString();
        String[] split = line.split("\t");
        // 获取感兴趣的订单信息
        String id = split[0];
        String pid = split[1];
        Long amount = Long.parseLong(split[2]);
        // 根据商品id获取商品名称
        String pName = pNames.get(pid);
        tableBean.setId(id);
        tableBean.setPid(pid);
        tableBean.setAmount(amount);
        tableBean.setpName(pName);
        out_k.set(id);
        // 写出数据
        context.write(tableBean,out_k);
    }
}
