package com.asiainfo.codis;

import com.asiainfo.codis.client.ClientToCodis;
import com.asiainfo.codis.client.ClientToCodisHelper;
import com.asiainfo.codis.schema.CodisHash;
import com.asiainfo.codis.schema.DataSchema;
import com.asiainfo.conf.CodisConfiguration;
import com.asiainfo.util.ExternalDataLoader;
import com.asiainfo.util.FileSplitUtil;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.PrefixFileFilter;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.apache.log4j.xml.DOMConfigurator;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

/**
 * Created by peng on 16/8/31.
 */
public class ImportData {
    private static Logger logger = Logger.getLogger(CodisConfiguration.class);

    public static void main(String[] args) {
        if (args.length != 1) {
            System.err.println("Usage: ImportData <schema configuration path>");
            System.exit(1);
        }

        String schemaPath = StringUtils.trimToEmpty(args[0]);

        if (!new File(schemaPath).exists()){
            System.err.println("Can not find file '" + schemaPath + "'.");
            System.exit(2);
        }

        DOMConfigurator.configure(CodisConfiguration.CONF_DIR + "log4j.xml");

        List<CodisHash> codisHashList = ExternalDataLoader.loadSchema(CodisConfiguration.CONF_DIR + File.separator + "schema.json");

        String inputDirStr = StringUtils.trimToEmpty(CodisConfiguration.getProperty().getProperty(CodisConfiguration.CODIS_INPUT_FILE_PATH));

        if (CodisConfiguration.getBoolean(CodisConfiguration.SPLIT_FILE_ENABLE, CodisConfiguration.DEFAULT_SPLIT_FILE_ENABLE)) {
            logger.info("Start to split source table data...");
            long start = System.currentTimeMillis();
            try {

                File inputDir = new File(inputDirStr);

                if (StringUtils.isNotEmpty(inputDirStr) && inputDir.exists()) {
                    File outputDir = new File(inputDirStr + File.separator + "output");

                    if (!outputDir.exists()) {
                        outputDir.mkdir();
                    }

                    for (CodisHash codisHash : codisHashList){
                        for (String tableName : codisHash.getSourceTableSchema().keySet()){
                            for (String fileName : inputDir.list(new PrefixFileFilter(tableName))) {

                                File sourceFile = new File(inputDirStr + File.separator + fileName);
                                long fileSize = CodisConfiguration.getLong(CodisConfiguration.CODIS_MAXIMUM_OPERATION_BYTE, CodisConfiguration.DEFAULT_CODIS_MAXIMUM_OPERATION_BYTE);

                                if (sourceFile.length() >= fileSize) {
                                    FileSplitUtil fileSplitUtil = new FileSplitUtil();
                                    long partitionSize = fileSplitUtil.getBlockFileSize(fileSize);
                                    logger.info("The partition size is " + partitionSize);
                                    List<String> parts = fileSplitUtil.splitBySize(sourceFile.getAbsolutePath(), outputDir.getAbsolutePath(), partitionSize);
                                    for (String part : parts) {
                                        logger.info("partName is:" + part);
                                    }
                                } else {
                                    FileUtils.copyFileToDirectory(sourceFile, outputDir);
                                }

                                logger.info("File size is " + sourceFile.length() + ", take " + (System.currentTimeMillis() - start) + " ms to split this file.");

                            }
                        }
                    }

                }


            } catch (Exception e) {
                logger.error(e);
            }
        }

        new ClientToCodis(codisHashList, inputDirStr).sendData();

    }
}
