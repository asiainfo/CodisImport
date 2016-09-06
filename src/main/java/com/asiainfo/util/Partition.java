package com.asiainfo.util;

/**
 * Created by peng on 16/9/1.
 */
public class Partition {
    private String name;
    private String firstRow;
    private String lastRow;
    private boolean firstIsFull;

    public String getName() {
        return name;
    }

    public String getFirstRow() {
        return firstRow;
    }

    public String getLastRow() {
        return lastRow;
    }

    public boolean isFirstIsFull() {
        return firstIsFull;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setFirstRow(String firstRow) {
        this.firstRow = firstRow;
    }

    public void setLastRow(String lastRow) {
        this.lastRow = lastRow;
    }

    public void setFirstIsFull(boolean firstIsFull) {
        this.firstIsFull = firstIsFull;
    }
}
