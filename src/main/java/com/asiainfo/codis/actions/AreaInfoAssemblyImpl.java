package com.asiainfo.codis.actions;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by peng on 16/9/9.
 */
public class AreaInfoAssemblyImpl extends MultiAssemblyImpl {
    private static Logger logger = Logger.getLogger(AreaInfoAssemblyImpl.class);
    @Override
    public Map<String, String> getMap() {
        HashMap<String, String> values = new HashMap();
        String[] hashKeys = super.codisHash.getHashFields();

        for (int j = 0; j < hashKeys.length; j++) {
            String hashKey = hashKeys[j].trim();
            String hashValue = getColumnValueFromSourceTableRow(hashKey);
            //String scene_code =
            if (hashValue != null) {

                if (hashKey.equals("scene_code")){
                    if (StringUtils.equalsIgnoreCase(hashValue, "A0")){
                        values.put("tour_area", getColumnValueFromSourceTableRow("area_code"));
                    }else if (StringUtils.equalsIgnoreCase(hashValue, "B0")){
                        values.put("security_area", getColumnValueFromSourceTableRow("sub_area_code"));
                    }else{
                        continue;
                    }
                }else if (hashKey.equals("area_code") || hashKey.equals("sub_area_code")){
                    continue;
                }else {
                    values.put(hashKey, hashValue);
                }
            }
        }

        logger.debug("Area info values : " + values);

        return values;
    }
}
