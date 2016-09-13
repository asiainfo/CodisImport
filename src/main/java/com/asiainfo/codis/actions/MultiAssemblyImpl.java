package com.asiainfo.codis.actions;

import com.asiainfo.codis.schema.CodisHash;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by peng on 16/9/8.
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
            super.codisHashKey = codisHash.getKeyPrefix() + ":" + codisHashKeyPostfix;
        }
        else {
            return false;
        }

        return true;
    }


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
