package com.atom.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author Atom
 */
public class HadoopHDFSDemo {
    private static final String HDFS_PATH = "hdfs://10.16.118.247:8020";
    //设置客户端身份，以具备权限在HDFS上进行操作
    private static final String HDFS_USER = "root";
    private static FileSystem fileSystem;


    private static final Logger LOGGER = LoggerFactory.getLogger(HadoopHDFSDemo.class);


    public static void main(String[] args) throws URISyntaxException, IOException, InterruptedException {
        Configuration configuration = new Configuration();
        // 这里我启动的是单节点的 Hadoop,所以副本系数设置为 1,默认值为 3
        configuration.set("dfs.replication", "1");
        configuration.set("dfs.client.use.datanode.hostname", "true");
// 设置客户端身份为管理员 root ,(root也可能不是管理员，这时就需要设置具体的管理员用户)
        System.setProperty("HADOOP__USER_NAME", "root");
        fileSystem = FileSystem.get(new URI(HDFS_PATH), configuration, HDFS_USER);

        if (createDirectory()) {
            LOGGER.info("create directory success.");
        }

        if (uploadLocalFile()) {
            LOGGER.info("upload file success.");
        }

        listFiles();

//

    }

    private static void listFiles() throws IOException {

        RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator = fileSystem.listFiles(new Path("/test002"), false);
        while (locatedFileStatusRemoteIterator.hasNext()){
            LocatedFileStatus next = locatedFileStatusRemoteIterator.next();
            System.err.println(next.getPath().getName());
        }
    }


    public static boolean createDirectory() throws IOException {
        return fileSystem.mkdirs(new Path("/test002/"),
                new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));
    }


    public static boolean uploadLocalFile() throws IOException {
        Path src = new Path("/Users/atom/hello.txt");
        // hdfs 会自动创建父文件夹
        Path dest = new Path("/test002/12.txt");

        //还可以设置是否删除本地文件，以及是否覆盖hdfs中的原有同名文件
        //默认不删除本地文件，覆盖远端文件
        try {
            fileSystem.copyFromLocalFile(src, dest);
        } catch (Exception e) {
            LOGGER.error("hdfs upload local file fail ", e);
            return false;
        }
        LOGGER.info("hdfs upload local file [{}] success ", src.getName());
//        fileSystem.copyFromLocalFile(true, src, dest);
//        fileSystem.copyFromLocalFile(true, true, src, dest);
        return true;
    }

}
