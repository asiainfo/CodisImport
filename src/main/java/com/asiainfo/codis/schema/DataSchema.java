package com.asiainfo.codis.schema;


import java.util.Arrays;


public class DataSchema {
    private String tablePrimaryKey;
    private String tableName;
    private String redisKey;

    private String[] header;

    private int tablePrimaryKeyIndex;

    public DataSchema setTablePrimaryKeyIndex(int tablePrimaryKeyIndex) {
        this.tablePrimaryKeyIndex = tablePrimaryKeyIndex;
        return this;
    }

    public DataSchema(String tableName) {
        this.tableName = tableName;
    }

    public DataSchema setTablePrimaryKey(String tablePrimaryKey) {
        this.tablePrimaryKey = tablePrimaryKey;
        return this;
    }

    public DataSchema setTableName(String tableName) {
        this.tableName = tableName;
        return this;
    }

    public DataSchema setRedisKey(String redisKey) {
        this.redisKey = redisKey;
        return this;
    }

    public DataSchema setHeader(String[] header) {
        this.header = header;
        return this;
    }

    public String getTablePrimaryKey() {
        return tablePrimaryKey;
    }

    public String getTableName() {
        return tableName;
    }

    public String getRedisKey() {
        return redisKey;
    }

    public String[] getHeader() {
        return header;
    }

    public int getTablePrimaryKeyIndex() {
        return tablePrimaryKeyIndex;
    }


    @Override
    public String toString() {
        return "DataSchema{" +
                "tablePrimaryKey='" + tablePrimaryKey + '\'' +
                ", tableName='" + tableName + '\'' +
                ", redisKey='" + redisKey + '\'' +
                ", header=" + Arrays.toString(header) +
                ", tablePrimaryKeyIndex=" + tablePrimaryKeyIndex +
                '}';
    }
}
