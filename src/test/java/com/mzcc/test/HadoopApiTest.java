package com.mzcc.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;


/**
 * 测试对hadoop 客户端Api相关操作
 */
public class HadoopApiTest {

//    @Before
//    void drive(){
//
//    }
//
//
//    @After
//    void close(){
//
//    }


//    @Test
//    public void testCreateDir() throws Exception {
//        Configuration configuration = new Configuration();
//
//        FileSystem system = FileSystem.get(new URI("hdfs://hadoop1:8020"), configuration, "root");
//
//        boolean mkdirs = system.mkdirs(new Path("/video"));
//
////        system.copyFromLocalFile(false, "D:\\迅雷下载\\RTP-020.avi", );
//    }
//
//
//    @Test
//    public void testUploadFile() throws Exception {
//        Configuration configuration = new Configuration();
//
//        FileSystem system = FileSystem.get(new URI("hdfs://hadoop1:8020"), configuration, "root");
//
//        system.copyFromLocalFile(false, new Path("D:\\迅雷下载\\RTP-020.avi"), new Path("/video/"));
//    }
//
//    @Test
//    public void testDownloadFile() throws Exception {
//        Configuration configuration = new Configuration();
//
//        FileSystem system = FileSystem.get(new URI("hdfs://hadoop1:8020"), configuration, "root");
//
//        system.copyToLocalFile(false, new Path("/video/RTP-020.avi"), new Path("E:\\"));
//
//        system.close();
//    }
//
//    @Test
//    public void testMoveRemoteFile() throws Exception {
//        Configuration configuration = new Configuration();
//
//        FileSystem system = FileSystem.get(new URI("hdfs://hadoop1:8020"), configuration, "root");
//
//        /*移动路径*/
//        system.rename(new Path("/video/RTP-020.avi"), new Path("/"));
//
//        /*重命名*/
//        system.rename(new Path("/RTP-020.avi"), new Path("/RTP020.avi"));
//        system.close();
//    }
//
//    @Test
//    public void testDeleteRemoteFile() throws Exception {
//        Configuration configuration = new Configuration();
//        FileSystem system = FileSystem.get(new URI("hdfs://hadoop1:8020"), configuration, "root");
//        system.delete(new Path("/RTP020.avi"), true);
//        system.close();
//    }
//
//    @Test
//    public void testGetAllRemoteFile() throws Exception {
//        Configuration configuration = new Configuration();
//        FileSystem system = FileSystem.get(new URI("hdfs://hadoop1:8020"), configuration, "root");
//        RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator = system.listFiles(new Path("/"), true);
//
//        while (locatedFileStatusRemoteIterator.hasNext()) {
//            LocatedFileStatus fileStatus = locatedFileStatusRemoteIterator.next();
//
//            System.out.println("========" + fileStatus.getPath() + "=========");
//            System.out.println(fileStatus.getPermission());
//            System.out.println(fileStatus.getOwner());
//            System.out.println(fileStatus.getGroup());
//            System.out.println(fileStatus.getLen());
//            System.out.println(fileStatus.getModificationTime());
//            System.out.println(fileStatus.getReplication());
//            System.out.println(fileStatus.getBlockSize());
//            System.out.println(fileStatus.getPath().getName());
//// 获取块信息
//            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
//            System.out.println(Arrays.toString(blockLocations));
//        }
//        system.close();
//    }
//
//
//    @Test
//    public void testListStatus() throws IOException, InterruptedException, URISyntaxException {
//// 1 获取文件配置信息
//        Configuration configuration = new Configuration();
//        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop1:8020"), configuration, "root");
//// 2 判断是文件还是文件夹
//        FileStatus[] listStatus = fs.listStatus(new Path("/"));
//        for (FileStatus fileStatus : listStatus) {
//// 如果是文件
//            if (fileStatus.isFile()) {
//                System.out.println("f:" + fileStatus.getPath().getName());
//            } else {
//                System.out.println("d:" + fileStatus.getPath().getName());
//            }
//        }
//// 3 关闭资源
//        fs.close();
//    }

}
