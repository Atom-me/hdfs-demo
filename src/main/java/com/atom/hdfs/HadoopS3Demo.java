package com.atom.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author Atom
 */
public class HadoopS3Demo {

    private static FileSystem fileSystem;

    public static final String AWS_S3_V4 = "com.amazonaws.services.s3.enableV4";
    public static final String FS_DEFAULTFS = "fs.defaultFS";
    /**
     * fs s3a endpoint
     */
    public static final String FS_S3A_ENDPOINT = "fs.s3a.endpoint";

    /**
     * fs s3a access key
     */
    public static final String FS_S3A_ACCESS_KEY = "fs.s3a.access.key";

    /**
     * fs s3a secret key
     */
    public static final String FS_S3A_SECRET_KEY = "fs.s3a.secret.key";


    private static final Logger LOGGER = LoggerFactory.getLogger(HadoopS3Demo.class);

    public static void main(String[] args) throws IOException {
        Configuration configuration = new Configuration();
        System.setProperty(AWS_S3_V4, "true");
        configuration.set(FS_DEFAULTFS, "s3a://liuyaming-hdfs-test");
        configuration.set(FS_S3A_ENDPOINT, "https://s3.cn-northwest-1.amazonaws.com.cn");
        configuration.set(FS_S3A_ACCESS_KEY, "");
        configuration.set(FS_S3A_SECRET_KEY, "");
        fileSystem = FileSystem.get(configuration);

        if (createDirectory()) {
            LOGGER.info("create directory success.");
        }

        if (uploadLocalFile()) {
            LOGGER.info("upload file success.");
        }
    }


    public static boolean createDirectory() throws IOException {
        return fileSystem.mkdirs(new Path("/resources/test002/"),
                new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));
    }


    public static boolean uploadLocalFile() throws IOException {
        Path src = new Path("/Users/atom/java_error_in_idea_42907.log");
        // S3 也会自动创建父文件夹
        Path dest = new Path("/dir2/java_error_in_idea_42907.log");

        //还可以设置是否删除本地文件，以及是否覆盖hdfs中的原有同名文件
        //默认不删除本地文件，覆盖远端文件
        try {
            fileSystem.copyFromLocalFile(src, dest);
        } catch (Exception e) {
            LOGGER.error("hdfs upload local file fail ", e);
            return false;
        }
//        fileSystem.copyFromLocalFile(true, src, dest);
//        fileSystem.copyFromLocalFile(true, true, src, dest);
        return true;
    }
}
