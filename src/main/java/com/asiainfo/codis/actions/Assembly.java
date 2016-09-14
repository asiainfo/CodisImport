package com.asiainfo.codis.actions;

import com.asiainfo.codis.schema.CodisHash;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

/**
 * 这个类负责将外部数据源中的一行数据转换为codis中的一个Hash
 */
public abstract class Assembly {
    private Logger logger = Logger.getLogger(Assembly.class);
    protected String sourceTableName;
    protected CodisHash codisHash;
    protected String codisHashKey;
    protected String[] row;
    protected Map<String, Map<String, String>> hmset = new HashMap<>();

    /**
     * 生成Hash的key，需要子类实现
     *
     * @param codisHash {@link CodisHash}封装了数据源和要生成的Hash的所有配置信息
     * @param sourceTableName 外部数据源的表名
     * @param row 表<code>sourceTableName</code>中的一行数据
     *
     * @return true 可以正确的生成key
     */
    public abstract boolean execute(CodisHash codisHash, String sourceTableName, String[] row);


    /**
     * 获取一个Hash的所有fields组成的map
     *
     * @return 一个Hash的所有fields组成的map
     */
    public Map<String, String> getMap() {
        HashMap<String, String> values = new HashMap();
        String[] hashKeys = codisHash.getHashFields();

        for (int j = 0; j < hashKeys.length; j++) {
            String hashValue = getColumnValueFromSourceTableRow(hashKeys[j].trim());
            if (hashValue != null) {
                values.put(hashKeys[j].trim(), hashValue);
            }
            else {
                logger.debug("Can not find " + hashKeys[j] + " from " + sourceTableName);
            }
        }

        logger.debug("All fields are :" + values);

        return values;
    }

    /**
     * 获取一个Hash的key
     *
     * @return 一个Hash的key
     */
    public String getKey() {
        logger.debug("key:" + codisHashKey);
        return codisHashKey;
    }


    /**
     * 在一行数据中找到指定列的值
     *
     * @param header 数据源表中某个列的名字
     *
     * @return 返回指定列的值，如果要查找的列在配置文件sourceTableSchema中没有定义那么返回null
     */
    protected String getColumnValueFromSourceTableRow(String header) {
        int index = codisHash.getSourceTableSchema().get(sourceTableName).indexOf(header);

        if (index < 0) {
            logger.trace("Can not find '" + header + "' from '" + codisHash.getSourceTableSchema().get(sourceTableName) + "'");
            return null;
        }
        return row[index].trim();
    }


    /**
     * 封装Hash的key和所有的fields
     *
     * @return 返回所有生成的Hash
     */
    public Map<String, Map<String, String>> getHmset() {
        hmset.put(getKey(), getMap());
        return hmset;
    }
}
