package com.jaon.mapreducedemo.writableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;

/*
 * 1、继承writable类
 * 2、空构造器
 * 3、设定数据
 * 4、重写write方法
 * 5、重写read方法
 */
public class FlowBean implements WritableComparable<FlowBean> {

    private Long downFlow;
    
    public Long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(Long downFlow) {
        this.downFlow = downFlow;
    }

    private long upFlow;
    public long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    private long sumFlow;

    public long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow() {
        this.sumFlow = this.upFlow + this.downFlow;
    }


    // 2 提供无参构造
    public FlowBean() {
    }

    @Override
    public void write(DataOutput out) throws IOException {
        // 依次将各个参数序列化
        out.writeLong(this.upFlow);
        out.writeLong(this.downFlow);
        out.writeLong(this.sumFlow);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        // 依次将各个参数反序列化
        this.upFlow = in.readLong();
        this.downFlow = in.readLong();
        this.sumFlow = in.readLong();
    }

    @Override
    public String toString() {
        return Long.toString(upFlow) + '\t' + Long.toString(downFlow) + '\t' + Long.toString(sumFlow);
    }

    @Override
    public int compareTo(FlowBean o) {

        // 实现排序，按照总流量的倒叙排序，如果总流量相等，则按照上传流量在倒叙排序
        if(this.sumFlow > o.sumFlow){
            return -1;
        }else if(this.sumFlow < o.sumFlow){
            return 1;
        }else{
            if(this.downFlow > o.downFlow){
                return -1;
            }else if(this.downFlow < o.downFlow){
                return 1;
            }else{
                return 0;
            }

        }
    }

}
