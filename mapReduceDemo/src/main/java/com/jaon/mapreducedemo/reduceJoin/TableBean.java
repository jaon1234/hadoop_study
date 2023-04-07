package com.jaon.mapreducedemo.reduceJoin;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TableBean implements WritableComparable<TableBean> {
    private String id;
    private String pid;
    private long amount;
    private  String pName;

    public String getpName() {
        return pName;
    }

    public void setpName(String pName) {
        this.pName = pName;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getPid() {
        return pid;
    }

    public void setPid(String pid) {
        this.pid = pid;
    }

    public long getAmount() {
        return amount;
    }

    public void setAmount(long amount) {
        this.amount = amount;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(id);
        out.writeUTF(pid);
        out.writeLong(amount);
        out.writeUTF(pName);

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        id = in.readUTF();
        pid = in.readUTF();
        amount = in.readLong();
        pName = in.readUTF();
    }

    @Override
    public int compareTo(TableBean o) {
        //倒序排列
        if(this.amount < o.amount){
            return 1;
        }else if(this.amount > o.amount){
            return -1;
        }else {
            return 0;
        }

    }

    @Override
    public String toString() {
        return id+"\t"+pName+"\t"+amount;
    }
}
