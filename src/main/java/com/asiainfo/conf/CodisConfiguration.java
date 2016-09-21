package com.asiainfo.conf;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import java.io.*;
import java.nio.file.Paths;
import java.util.*;

/**
 * Created by peng on 16/8/30.
 */
public class CodisConfiguration {
    private static Properties properties = new Properties();
    private static Logger logger = Logger.getLogger(CodisConfiguration.class);
    private final static String CONFIG_FILE = "codis.properties";
    public final static String CODIS_IMPORT_THRESHOLD = "codis.import.threshold";
    public final static int CODIS_IMPORT_THRESHOLD_DEFAULT = 10000;
    public final static String CODIS_ADDRESS = "codis.address";
    public final static String DEFAULT_SEPARATOR = ",";
    public final static String CODIS_CLIENT_THREAD_COUNT = "codis.client.thread-count";
    public static int DEFAULT_CODIS_CLIENT_THREAD_COUNT = 1;
    public final static String CODIS_CLIENT_LIVENESS_MONITOR_EXPIRY_INTERVAL_MS = "codis.client.liveness-monitor.expiry-interval-ms";
    public final static long DEFAULT_CODIS_CLIENT_LIVENESS_MONITOR_EXPIRY_INTERVAL_MS = 10000;
    public final static String CODIS_MAXIMUM_OPERATION_BYTE = "codis.maximum-operation-byte";
    public final static long DEFAULT_CODIS_MAXIMUM_OPERATION_BYTE = 1024 * 1024 * 1024; //1M
    public final static String SPLIT_FILE_ENABLE = "split.file.enable";
    public final static boolean DEFAULT_SPLIT_FILE_ENABLE = false;
    public final static String CODIS_INPUT_FILE_PATH = "codis.input.file.path";
    public static String CONF_DIR = "";
    public static String HOME_PATH = "";



    static {
        DEFAULT_CODIS_CLIENT_THREAD_COUNT = Runtime.getRuntime().availableProcessors();

        HOME_PATH = Paths.get(CodisConfiguration.class.getProtectionDomain().getCodeSource().getLocation().getPath()).getParent().getParent().toString();

        CONF_DIR = HOME_PATH + File.separator + "conf" + File.separator;

        try {
            properties.load(new FileInputStream(HOME_PATH + File.separator + "conf" + File.separator + CONFIG_FILE));
        } catch (FileNotFoundException fnf) {
            logger.error("No configuration file " + CONFIG_FILE + " found in classpath.", fnf);
        } catch (IOException ie) {
            logger.error("Can't read configuration file " + CONFIG_FILE, ie);
        }
    }

    public static Properties getProperty(){
        return properties;
    }

    public static int getInt(String name, int defaultValue) {
        String valueString = StringUtils.trim(properties.getProperty(name));
        if (StringUtils.isEmpty(valueString))
            return defaultValue;

        return Integer.parseInt(valueString) > 0 ? Integer.parseInt(valueString) : defaultValue;
    }

    public static long getLong(String name, long defaultValue) {
        String valueString = StringUtils.trim(properties.getProperty(name));
        if (StringUtils.isEmpty(valueString))
            return defaultValue;

        return Long.parseLong(valueString) > 0 ? Long.parseLong(valueString) : defaultValue;
    }

    public static List<String> getStringArray(String name, String separator) {
        String valueString = StringUtils.trim(properties.getProperty(name));
        if (StringUtils.isEmpty(valueString))
            return Collections.EMPTY_LIST;

        return Arrays.asList(valueString.split(separator));
    }

    public static boolean getBoolean(String name, boolean defaultValue) {
        String valueString = StringUtils.trim(properties.getProperty(name));
        if (null == valueString || valueString.isEmpty()) {
            return defaultValue;
        }

        if (StringUtils.equalsIgnoreCase("true", valueString))
            return true;
        else if (StringUtils.equalsIgnoreCase("false", valueString))
            return false;
        else return defaultValue;
    }

}
