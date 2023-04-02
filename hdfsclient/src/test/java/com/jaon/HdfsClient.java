package com.jaon;


import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.apache.hadoop.conf.Configuration;

public class HdfsClient {

    private static FileSystem fs = null;
    
    @Before
    public void init() throws IOException, InterruptedException, URISyntaxException {
        // 1 配置文件
        Configuration configuration = new Configuration();
        // configuration.setInt("dfs.replication", 1);
        fs = FileSystem.get(new URI("hdfs://hadoop100:8020"), configuration, "hadoop");
        System.out.println(fs.getScheme());

    }
    @After
    public void close() throws IOException {
        fs.close();
    }

    @Test
    public void mkdirs() throws IOException, URISyntaxException, InterruptedException {
        // 2 创建目录
        fs.mkdirs(new Path("/xiyou/xiaomi/"));
    }

    @Test
    public void testPut() throws IOException {
        System.out.println("hello");
        Path srcPath = new Path("E:\\hadoop-3.1.0\\pom - 副本.xml");
        Path dsPath = new Path("/wcinput/");
        fs.copyFromLocalFile(false, true, srcPath, dsPath);
    }
    
    // 从hdfs当中获取数据，并且拿到校验码
    @Test
    public void testGet() throws IllegalArgumentException, IOException {
        fs.copyToLocalFile(false, new Path("/a.txt"), new Path("E:\\hadoop-3.1.0\\"), false);        
    }

    @Test
    public void fileDetails() throws FileNotFoundException, IllegalArgumentException, IOException {
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
        while(listFiles.hasNext()){
            LocatedFileStatus fileStatus = listFiles.next();
            System.out.println("===="+fileStatus.getPath()+"=========");
            System.out.println(fileStatus.getPermission());
            System.out.println(fileStatus.getOwner());
            System.out.println(fileStatus.getGroup());
            System.out.println(fileStatus.getLen());
            System.out.println(fileStatus.getModificationTime());
            System.out.println(fileStatus.getReplication());
            System.out.println(fileStatus.getBlockSize());
            System.out.println(fileStatus.getPath().getName());
            System.out.println(Arrays.toString(fileStatus.getBlockLocations()));
        }
    }
}
