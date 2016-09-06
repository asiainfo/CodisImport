package com.asiainfo.codis;

import com.asiainfo.codis.client.ClientToCodis;
import com.asiainfo.codis.client.ClientToCodisHelper;
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
        DOMConfigurator.configure(CodisConfiguration.CONF_DIR + "log4j.xml");

        List<DataSchema> schemaList = ExternalDataLoader.loadSchema(CodisConfiguration.CONF_DIR + File.separator + "schema.properties");
        String inputDirStr = StringUtils.trimToEmpty(CodisConfiguration.getProperty().getProperty(CodisConfiguration.CODIS_INPUT_FILE_PATH));

        if (CodisConfiguration.getBoolean(CodisConfiguration.SPLIT_FILE_ENABLE, CodisConfiguration.DEFAULT_SPLIT_FILE_ENABLE)) {
            long start = System.currentTimeMillis();
            try {

                File inputDir = new File(inputDirStr);

                if (StringUtils.isNotEmpty(inputDirStr) && inputDir.exists()) {
                    File outputDir = new File(inputDirStr + File.separator + "output");

                    if (!outputDir.exists()) {
                        outputDir.mkdir();
                    }


                    for (DataSchema dataSchema : schemaList) {

                        for (String fileName : inputDir.list(new PrefixFileFilter(dataSchema.getTableName()))) {

                            File sourceFile = new File(inputDirStr + File.separator + fileName);
                            long fileSize = CodisConfiguration.getLong(CodisConfiguration.CODIS_MAXIMUM_OPERATION_BYTE, CodisConfiguration.DEFAULT_CODIS_MAXIMUM_OPERATION_BYTE);

                            if (sourceFile.length() >= fileSize) {
                                FileSplitUtil fileSplitUtil = new FileSplitUtil();
                                int blockSize = fileSplitUtil.getBlockFileSize(fileSize);
                                logger.info("The block size is " + blockSize);
                                List<String> parts = fileSplitUtil.splitBySize(sourceFile.getAbsolutePath(), outputDir.getAbsolutePath(), blockSize);
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


            } catch (Exception e) {
                logger.error(e);
            }
        }

        new ClientToCodis(schemaList, inputDirStr + File.separator + "output").sendData();

    }
}
