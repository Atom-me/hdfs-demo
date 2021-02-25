package com.atom.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;

class HdfsTest {
    private static final String HDFS_PATH = "hdfs://datago:9000";
    private static final String HDFS_USER = "root";
    private static FileSystem fileSystem;


    @BeforeEach
    public void prepare() throws URISyntaxException, IOException, InterruptedException {
        Configuration configuration = new Configuration();
        // 这里我启动的是单节点的 Hadoop,所以副本系数设置为 1,默认值为 3
        configuration.set("dfs.replication", "1");
        configuration.set("dfs.client.use.datanode.hostname", "true");
        fileSystem = FileSystem.get(new URI(HDFS_PATH), configuration, HDFS_USER);
    }

    @AfterEach
    public void destroy() {
        fileSystem = null;
    }

    @Test
    public void testMakeDirWithDirDefaultPermission() throws IOException {
        boolean mkdirs = fileSystem.mkdirs(new Path("/atom/test001/"), FsPermission.getDirDefault());
        System.err.println(mkdirs);
    }

    /**
     * FsPermission(FsAction u, FsAction g, FsAction o)
     * 三个参数分别对应：创建者权限，同组其他用户权限，其他用户权限，
     * 权限值定义在 FsAction 枚举类中。
     *
     * @throws Exception
     */

    @Test
    public void testMkDirWithPermission() throws Exception {
        boolean mkdirs = fileSystem.mkdirs(new Path("/atom/test002/"),
                new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));
        System.err.println(mkdirs);
    }


    /**
     * 创建文件，并写入内容，会自动创建父文件夹
     *
     * @throws Exception
     */
    @Test
    public void testCreateFileAndWriteData() throws Exception {
        // 如果文件存在，默认会覆盖, 可以通过第二个参数进行控制。第三个参数可以控制使用缓冲区的大小
        FSDataOutputStream out = fileSystem.create(new Path("/dir1/dir1-1/a.txt"),
                true, 4096);
        out.write("hello hadoop!".getBytes());
        out.write("hello spark!".getBytes());
        out.write("hello flink!".getBytes());
        // 强制将缓冲区中内容刷出
        out.flush();
        out.close();
    }


    /**
     * 上传文件，会自动创建父文件夹
     * 还可以设置是否删除本地文件，以及是否覆盖hdfs中的原有同名文件
     *
     * @throws IOException
     */
    @Test
    public void testUploadFile() throws IOException {
        Path src = new Path("/Users/atom/ubuntu-wallpapers/11.png");
        Path dest = new Path("/dir2/11.png");

        //还可以设置是否删除本地文件，以及是否覆盖hdfs中的原有同名文件
        //默认不删除本地文件，覆盖远端文件
        fileSystem.copyFromLocalFile(src, dest);
//        fileSystem.copyFromLocalFile(true, src, dest);
//        fileSystem.copyFromLocalFile(true, true, src, dest);
    }


    /**
     * 使用IO流上传文件，需要首先创建文件
     *
     * @throws IOException
     */
    @Test
    public void testUploadFileUseIO() throws IOException {
        FileInputStream srcInputStream = new FileInputStream("/Users/atom/ubuntu-wallpapers/11.png");
        Path dest = new Path("/dir2/33.png");
        FSDataOutputStream fsDataOutputStream = fileSystem.create(dest);

        /**
         * close 参数： 表示copy完成是否关闭输入/输出流
         */
        IOUtils.copyBytes(srcInputStream, fsDataOutputStream, 1024, true);

    }


    /**
     * 下载文件，
     * 还可以设置是否删除源文件，
     * 以及是否使用useRawLocalFileSystem作为本地文件系统：即是否开启文件效验（生成.22.png.crc文件）
     *
     * @throws IOException
     */
    @Test
    public void testDownloadFile() throws IOException {
        Path src = new Path("/dir2/11.png");
        Path dest = new Path("/Users/atom/ubuntu-wallpapers/22.png");

        fileSystem.copyToLocalFile(src, dest);


        //还可以设置是否删除hdfs中的源文件
//        fileSystem.copyToLocalFile(true, src, dest);

        /**
         *    boolean delSrc 指是否将原文件删除
         *
         *     Path src 指要下载的文件路径
         *
         *     Path dst 指将文件下载到的路径
         *
         *     boolean useRawLocalFileSystem 是否使用useRawLocalFileSystem作为本地文件系统：即是否开启文件效验（生成.22.png.crc文件）
         *
         *
         *     在使用LocalFileSystem写文件时，会透明的创建一个.filename.crc的文件。
         *     校验文件大小的字节数由io.bytes.per.checksum属性设置，默认是512bytes,即每512字节就生成一个CRC-32校验 和。
         *     .filename.crc文件会存 io.bytes.per.checksum的信息。
         *     在读取的时候，会根据此文件进行校验。
         *     事实上LocalFileSystem是通过继承 ChecksumFileSystem实现校验的工作。
         */
//        fileSystem.copyToLocalFile(false, src, dest, false);
    }


    /**
     * 使用IO流下载文件，
     *
     * @throws IOException
     */
    @Test
    public void testDownloadFileUseIO() throws IOException {
        //创建本地输出流，保存内容
        OutputStream fos = new FileOutputStream("/Users/atom/ubuntu-wallpapers/55.png");

        //创建hdfs输入流
        Path getPath = new Path("/dir2/11.png");
        FSDataInputStream fis = fileSystem.open(getPath);

        /**
         * close 参数： 表示copy完成是否关闭输入/输出流
         */
//        IOUtils.copyBytes(fis, System.out, 1024, true);
        IOUtils.copyBytes(fis, fos, 1024, true);

    }


    /**
     * 删除目录
     * 如果目录非空，需要设置递归删除，否则删除失败`/dir2 is non empty': Directory is not empty
     */
    @Test
    public void testDeleteDir() throws IOException {
        Path deletePath = new Path("/dir2");
        boolean delete = fileSystem.delete(deletePath, true);
        System.err.println(delete);
    }


    /**
     * 查看指定目录下的文件属性
     *
     * @throws IOException
     */
    @Test
    public void testListFileMetaData() throws IOException {
        RemoteIterator<LocatedFileStatus> pathList = fileSystem.listFiles(new Path("/dir2"), false);
        while (pathList.hasNext()) {
            LocatedFileStatus fileStatus = pathList.next();
            System.out.println("文件名：" + fileStatus.getPath().getName());
            System.out.println("文件权限：" + fileStatus.getPermission());
            System.out.println("文件属主：" + fileStatus.getOwner());
            System.out.println("文件属组：" + fileStatus.getGroup());
            System.out.println("文件大小Byte：" + fileStatus.getLen());
            System.out.println("文件的block的大小blockSize:" + fileStatus.getBlockSize() / (1024 * 1024) + "Mb");
            //获取文件的block地址
            BlockLocation[] bl = fileStatus.getBlockLocations();
            for (BlockLocation blockLocation : bl) {
                //获取每个block 的偏移地址
                System.out.println("offset：" + blockLocation.getOffset());
                String[] hosts = blockLocation.getHosts();
                for (String host : hosts) {
                    System.out.println("当前副本的block所在的所有datanode的主机名：" + host);
                }
            }
            System.out.println("===========================");
        }
    }


    /**
     * 查看文件或者文件夹属性
     * <p>
     * FileStatus 可以判读文件是文件还是目录
     *
     * @throws IOException
     */
    @Test
    public void testDirOrFileMetaData() throws IOException {
        Path path = new Path("/");
        //获取FileSstatus对象
        FileStatus[] fileStatuses = fileSystem.listStatus(path);
        //通过Filestatus对象获取文件或者目录的属性
        for (FileStatus f : fileStatuses) {
            System.out.println("是否是目录:" + f.isDirectory());
            System.out.println("文件名:" + f.getPath().getName());
            System.out.println("权限:" + f.getPermission());
            System.out.println("大小:" + f.getLen());
            System.out.println("==========================");
        }

    }

}
