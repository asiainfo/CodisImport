package com.asiainfo.codis.actions;

import com.asiainfo.codis.schema.CodisHash;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

/**
 * 这个类处理在foreignKeys中只配置了多于一个字段的情况
 *
 */
public class MultiAssemblyImpl extends Assembly {
    private Logger logger = Logger.getLogger(MultiAssemblyImpl.class);

    @Override
    public boolean execute(CodisHash codisHash, String sourceTableName, String[] row) {
        super.codisHash = codisHash;
        super.sourceTableName = sourceTableName;
        super.row = row;

        if (codisHash.getForeignKeys().length < 2) {
            logger.error("The number of foreignkeys are less than two.");
            return false;

        }

        String codisHashKeyPostfix = getCodisHashKeyPostfix(codisHash.getForeignKeys());
        if (codisHashKeyPostfix != null){
            super.codisHashKey = codisHash.getKeyPrefix() + super.codisHash.getKeySeparator() + codisHashKeyPostfix;
        }
        else {
            logger.error("Can not find the columns '" + codisHash.getForeignKeys() + "' from table '" + super.sourceTableName + "'");
            return false;
        }

        return true;
    }

    /**
     * 对于foreignKeys中多个字段的情况，需要将多个字段拼接成Hash的key的后缀
     * 如果用户配置的表中没有foreignKeys中配置的字段，只要任意一个找不到就返回null
     */
    protected String getCodisHashKeyPostfix(String[] headers) {
        StringBuffer bs = new StringBuffer();
        for (String header : headers){
            int index = super.codisHash.getSourceTableSchema().get(super.sourceTableName).indexOf(header);
            if (index < 0){
                logger.warn("Can not find '" + header + "' from '" + super.codisHash.getSourceTableSchema().get(super.sourceTableName) + "'");
                return null;
            }

            bs.append(super.row[index].trim()).append(super.codisHash.getForeignKeysSeparator());

        }

        return StringUtils.removeEnd(bs.toString(), super.codisHash.getForeignKeysSeparator());
    }

}
