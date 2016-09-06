package com.asiainfo.util;

/**
 * Created by peng on 16/8/30.
 */
import com.asiainfo.conf.CodisConfiguration;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

public class FileSplitUtil {
    private final static Logger log = LogManager.getLogger(FileSplitUtil.class);

    public List<String> splitBySize(String fileName, String outputPath, long byteSize)
            throws IOException, InterruptedException {
        List<String> parts = new ArrayList();
        File file = new File(fileName);
        int count = (int) Math.ceil(file.length() / (double) byteSize);
        int countLen = (count + "").length();
        RandomAccessFile raf = new RandomAccessFile(fileName, "r");
        long totalLen = raf.length();
        CountDownLatch latch = new CountDownLatch(count);

        for (int i = 0; i < count; i++) {
            String partFileName = outputPath + File.separator + file.getName() + "-"
                    + leftPad((i + 1) + "", countLen, '0');
            long readSize = byteSize;
            long startPos=(long)i * byteSize;
            long nextPos=(long)(i+1) * byteSize;
            if(nextPos>totalLen){
                readSize= (int) (totalLen-startPos);
            }
            new Thread(new SplitRunnable(readSize, startPos, partFileName, file, latch)).start();
            parts.add(partFileName);
        }
        latch.await();

        mergeRow(parts);
        return parts;
    }


    private void mergeRow(List<String> parts) {
        List<Partition> partFiles = new ArrayList();
        try {
            //组装被切分表对象
            for (int i=0;i<parts.size();i++) {
                String partFileName=parts.get(i);
                File splitFileTemp = new File(partFileName);
                if (splitFileTemp.exists()) {
                    Partition partFile = new Partition();
                    BufferedReader reader=new BufferedReader(new InputStreamReader(new FileInputStream(splitFileTemp),"gbk"));
                    String firstRow = reader.readLine();
                    String secondRow = reader.readLine();
                    String endRow = readLastLine(partFileName);
                    partFile.setName(partFileName);
                    partFile.setFirstRow(firstRow);
                    partFile.setLastRow(endRow);
                    if(i >= 1){
                        String prePartFile=parts.get(i - 1);
                        String preEndRow = readLastLine(prePartFile);
                        partFile.setFirstIsFull(getCharCount(firstRow+preEndRow)>getCharCount(secondRow));
                    }

                    partFiles.add(partFile);
                    reader.close();
                }
            }
            //进行需要合并的行的写入
            for (int i = 0; i < partFiles.size() - 1; i++) {
                Partition partFile = partFiles.get(i);
                Partition partFileNext = partFiles.get(i + 1);
                StringBuilder sb = new StringBuilder();
                if (partFileNext.isFirstIsFull()) {
                    sb.append("\r\n");
                    sb.append(partFileNext.getFirstRow());
                } else {
                    sb.append(partFileNext.getFirstRow());
                }
                writeLastLine(partFile.getName(),sb.toString());
            }
        } catch (Exception e) {
            log.error(e.getMessage());
        }
    }

    private int getCharCount(String s) {
        return StringUtils.countMatches(s, CodisConfiguration.DEFAULT_SEPARATOR);
    }

    private String readLastLine(String filename) throws IOException {
        // 使用RandomAccessFile , 从后找最后一行数据
        RandomAccessFile raf = new RandomAccessFile(filename, "r");
        long len = raf.length();
        String lastLine = "";
        if(len!=0L) {
            long pos = len - 1;
            while (pos > 0) {
                pos--;
                raf.seek(pos);
                if (raf.readByte() == '\n') {
                    lastLine = raf.readLine();
                    lastLine=new String(lastLine.getBytes("8859_1"), "gbk");
                    break;
                }
            }
        }
        raf.close();
        return lastLine;
    }

    private void writeLastLine(String fileName,String lastString){
        try {
            // 打开一个随机访问文件流，按读写方式
            RandomAccessFile randomFile = new RandomAccessFile(fileName, "rw");
            // 文件长度，字节数
            long fileLength = randomFile.length();
            //将写文件指针移到文件尾。
            randomFile.seek(fileLength);
            //此处必须加gbk，否则会出现写入乱码
            randomFile.write(lastString.getBytes("gbk"));

            randomFile.toString();
            randomFile.close();

        } catch (IOException e) {
            log.error(e.getMessage());
        }
    }

    public static String leftPad(String str, int length, char ch) {
        if (str.length() >= length) {
            return str;
        }
        char[] chs = new char[length];
        Arrays.fill(chs, ch);
        char[] src = str.toCharArray();
        System.arraycopy(src, 0, chs, length - src.length, src.length);
        return new String(chs);
    }

    public long getBlockFileSize(long fileSize){
        long x = 2;
        long result = 0;

        while(true){
            if(x == fileSize){
                result = x;
                break;
            }if(x < fileSize){
                result = x;
                x = 2 * x;
            }else{
                break;
            }
        }
        return result;
    }

}
