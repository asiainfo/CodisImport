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
        String[] hashKeys = codisHash.getHashKeys();

        for (int j = 0; j < hashKeys.length; j++) {
            String hashKey = hashKeys[j].trim();
            String hashValue = getTargetValue(hashKey);
            if (hashValue != null) {

                if (hashKey.equals("area_code") ){
                    if (StringUtils.startsWithIgnoreCase(hashValue, "A")){
                        values.put("security_area", hashValue);
                    }else if (StringUtils.startsWithIgnoreCase(hashValue, "B")){
                        values.put("tour_area", hashValue);
                    }
                }
                else if (hashKey.equals("sub_area_code") ){
                    if (StringUtils.startsWithIgnoreCase(hashValue, "A")){
                        values.put("security_sub_area", hashValue);
                    }else if (StringUtils.startsWithIgnoreCase(hashValue, "B")){
                        values.put("tour_sub_area", hashValue);
                    }
                }
                else{
                    values.put(hashKey, hashValue);
                }
            }
        }

        logger.debug("Area info values : " + values);

        return values;
    }
}
