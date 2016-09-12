package com.asiainfo.codis.schema;

import java.util.List;
import java.util.Map;

/**
 * Created by peng on 16/9/8.
 */
public class SourceTable {
    private String name;
    private List<String> header;

    public void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }


    @Override
    public String toString() {
        return "SourceTable{" +
                "name='" + name + '\'' +
                ", header=" + header +
                '}';
    }

    public List<String> getHeader() {
        return header;
    }

    public void setHeader(List<String> header) {
        this.header = header;
    }
}
