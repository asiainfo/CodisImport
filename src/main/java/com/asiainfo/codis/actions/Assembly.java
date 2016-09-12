package com.asiainfo.codis.actions;

import com.asiainfo.codis.schema.CodisHash;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by peng on 16/9/8.
 */
public abstract class Assembly {
    private Logger logger = Logger.getLogger(Assembly.class);
    protected String sourceTableName;
    protected CodisHash codisHash;
    protected String codisHashKey;
    protected String[] row;
    public abstract boolean execute(CodisHash codisHash, String sourceTableName, String[] row);


    public Map<String, String> getMap() {
        HashMap<String, String> values = new HashMap();
        String[] hashKeys = codisHash.getHashKeys();

        for (int j = 0; j < hashKeys.length; j++) {
            String hashValue = getTargetValue(hashKeys[j].trim());
            if (hashValue != null) {
                values.put(hashKeys[j].trim(), hashValue);
            }
        }

        System.out.println(values);

        return values;
    }


    public String getKey(){
        logger.debug("key:" + codisHashKey);
        return codisHashKey;
    }

    protected String getTargetValue(String header) {
        int index = codisHash.getSourceTableSchema().get(sourceTableName).indexOf(header);

        if (index < 0){
            logger.warn("Can not find '" + header + "' from '" + codisHash.getSourceTableSchema().get(sourceTableName) + "'");
            return null;
        }
        return row[index].trim();
    }
}
