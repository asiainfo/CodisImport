package com.asiainfo.util;

import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by peng on 16/9/9.
 */
public class CommonUtil {

    public static String listToString(List<String> list, String separator){
        StringBuilder bs = new StringBuilder();
        list.forEach(item->bs.append(item).append(separator));
        return StringUtils.removeEnd(bs.toString(), separator);
    }

    public static String arrayToString(String[] array, String separator){
        return listToString(Arrays.asList(array), separator);
    }
}
