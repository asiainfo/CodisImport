package com.asiainfo.util;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.concurrent.CountDownLatch;

/**
 * Created by peng on 16/9/1.
 */
public class SplitRunnable implements Runnable {
    private final static Logger log = LogManager.getLogger(SplitRunnable.class);


    private int byteSize;
    private String partitionName;
    private File originFile;
    private long startPos;
    private CountDownLatch latch;

    public SplitRunnable(int byteSize, long startPos, String partitionName,
                         File originFile, CountDownLatch latch) {
        this.startPos = startPos;
        this.byteSize = byteSize;
        this.partitionName = partitionName;
        this.originFile = originFile;
        this.latch = latch;
    }

    public void run() {
        RandomAccessFile rFile;
        OutputStream os;
        try {
            rFile = new RandomAccessFile(originFile, "r");
            byte[] b = new byte[byteSize];
            rFile.seek(startPos);// 移动指针到每“段”开头



            int s = rFile.read(b);
            os = new FileOutputStream(partitionName);
            os.write(b, 0, s);



            os.flush();
            os.close();
            latch.countDown();
        } catch (IOException e) {
            log.error(e.getMessage());
            latch.countDown();
        }
    }

}
