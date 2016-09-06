package com.asiainfo.util;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.concurrent.CountDownLatch;

/**
 * Created by peng on 16/9/1.
 */
public class SplitRunnable implements Runnable {
    private final static Logger logger = LogManager.getLogger(SplitRunnable.class);


    private static final int byteSize = 1024 * 1024;
    private long partitionSize;
    private String partitionName;
    private File originFile;
    private long startPos;
    private CountDownLatch latch;

    public SplitRunnable(long partitionSize, long startPos, String partitionName,
                         File originFile, CountDownLatch latch) {
        this.startPos = startPos;
        this.partitionSize = partitionSize;
        this.partitionName = partitionName;
        this.originFile = originFile;
        this.latch = latch;
    }

    @Override
    public void run() {
        RandomAccessFile rFile;
        OutputStream os;
        try {
            rFile = new RandomAccessFile(originFile, "r");

            byte[] buf;

            if (partitionSize < byteSize){
                buf = new byte[(int) partitionSize];
            }
            else {
                buf = new byte[byteSize];
            }

            rFile.seek(startPos);// 移动指针到每“段”开头

            os = new FileOutputStream(partitionName);

            long count = 0;
            int hasRead;

            while(count < partitionSize) {

                hasRead = rFile.read(buf);

                logger.info("====" + hasRead + "-" + Thread.currentThread().getName());

                os.write(buf, 0, hasRead);
                if (hasRead <= 0){
                    break;
                }

                count = count + hasRead;
            }

            os.flush();
            os.close();

        } catch (IOException e) {
            logger.error(e.getMessage());
        }finally {
            latch.countDown();
        }
    }

}
