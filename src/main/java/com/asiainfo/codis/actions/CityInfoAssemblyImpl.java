package com.asiainfo.codis.actions;

import com.asiainfo.codis.schema.CodisHash;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by peng on 16/9/9.
 */
public class CityInfoAssemblyImpl extends SingleForeignKeysAssemblyImpl {
    private static Logger logger = Logger.getLogger(CityInfoAssemblyImpl.class);

    @Override
    public Map<String, Map<String, String>> getHmset() {
        String hmsetKey = super.getKey();
        Map<String, String> hmsetValue = super.getMap();

        if (hmsetKey.length() == 9){
            for (int i = 0; i <= 9 ; i++){
                super.hmset.put(hmsetKey + i, hmsetValue);
            }
        }
        else {
            super.hmset.put(hmsetKey, hmsetValue);
        }
        return super.hmset;
    }
}
