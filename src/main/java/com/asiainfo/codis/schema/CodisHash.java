package com.asiainfo.codis.schema;

import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Created by peng on 16/9/8.
 */
public class CodisHash {
    private String keyPrefix;
    private String[] foreignKeys;
    private String keySeparator = ":";
    private String foreignKeysSeparator = "_";

    private Map<String, List<String>> sourceTableSchema; //name, header

    private String[] hashKeys;

    private String handlerClass;

    public static final String SIGLE_FOREIGN_KEY_HADLE_CLASS = "com.asiainfo.codis.actions.SingleForeignKeysAssemblyImpl";
    public static final String MULTI_FOREIGN_KEY_HADLE_CLASS = "com.asiainfo.codis.actions.MultiAssemblyImpl";


    public String getCodisHashKey(){
        return keyPrefix + keySeparator + listToString(foreignKeys);
    }

    private String listToString(String[] list){
        StringBuilder bs = new StringBuilder();

        for(String v : list){
            bs.append(v).append(foreignKeysSeparator);
        }


        return StringUtils.removeEnd(bs.toString(), foreignKeysSeparator);
    }




    public String getKeyPrefix() {
        return keyPrefix;
    }


    public void setKeyPrefix(String keyPrefix) {
        this.keyPrefix = keyPrefix;
    }

    public void setForeignKeys(String[] foreignKeys) {
        this.foreignKeys = foreignKeys;
    }

    public void setSourceTableSchema(Map<String, List<String>> sourceTableSchema) {
        this.sourceTableSchema = sourceTableSchema;
    }

    public void setHashKeys(String[] hashKeys) {
        this.hashKeys = hashKeys;
    }

    public void setHandlerClass(String handlerClass) {
        this.handlerClass = handlerClass;
    }

    @Override
    public String toString() {
        return "CodisHash{" +
                "keyPrefix='" + keyPrefix + '\'' +
                ", foreignKeys=" + Arrays.toString(foreignKeys) +
                ", sourceTableSchema=" + sourceTableSchema +
                ", hashKeys=" + Arrays.toString(hashKeys) +
                ", handlerClass='" + handlerClass + '\'' +
                '}';
    }

    public String[] getForeignKeys() {
        return foreignKeys;
    }

    public Map<String, List<String>> getSourceTableSchema() {
        return sourceTableSchema;
    }

    public String[] getHashKeys() {
        return hashKeys;
    }

    public String getHandlerClass() {
        return StringUtils.trim(handlerClass);
    }

    public String getForeignKeysSeparator() {
        return foreignKeysSeparator;
    }
}
