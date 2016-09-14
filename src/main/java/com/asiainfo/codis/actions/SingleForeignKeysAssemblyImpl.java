package com.asiainfo.codis.actions;

import com.asiainfo.codis.schema.CodisHash;
import org.apache.log4j.Logger;

/**
 * 这个类只处理在foreignKeys中只配置了一个字段的情况
 *
 */
public class SingleForeignKeysAssemblyImpl extends Assembly {
    private Logger logger = Logger.getLogger(SingleForeignKeysAssemblyImpl.class);

    @Override
    public boolean execute(CodisHash codisHash, String sourceTableName, String[] row) {
        super.codisHash = codisHash;
        super.sourceTableName = sourceTableName;
        super.row = row;

        if (codisHash.getForeignKeys().length != 1) {
            logger.error("The number of foreignkeys dose not equal 1.");
            return false;
        }


        String codisHashKeyPostfix = getColumnValueFromSourceTableRow(codisHash.getForeignKeys()[0]);
        if (codisHashKeyPostfix != null){
            super.codisHashKey = codisHash.getKeyPrefix() + super.codisHash.getKeySeparator() + codisHashKeyPostfix;
        }else {
            logger.error("Can not find the column '" + codisHash.getForeignKeys()[0] + "' from table '" + super.sourceTableName + "'");
            return false;
        }

        return true;
    }

}
