package com.asiainfo.codis.client;

import com.asiainfo.codis.actions.Assembly;
import com.asiainfo.codis.schema.CodisHash;
import com.asiainfo.conf.CodisConfiguration;
import io.codis.jodis.JedisResourcePool;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import redis.clients.jedis.*;

import java.util.List;
import java.util.Map;
import java.util.concurrent.RecursiveAction;

/**
 * Created by peng on 16/8/31.
 */
public class ClientToCodisHelper extends RecursiveAction {
    private Logger logger = Logger.getLogger(ClientToCodisHelper.class);

    private JedisResourcePool jedisPool;
    private List<String> dataList;

    private CodisHash codisHash;
    private String sourceTableName;

    private int start;
    private int end;

    public ClientToCodisHelper(List<String> dataList, CodisHash codisHash, String sourceTableName, JedisResourcePool jedisPool, int start, int end) {
        this.dataList = dataList;
        this.codisHash = codisHash;
        this.jedisPool = jedisPool;
        this.start = start;
        this.end = end;
        this.sourceTableName = sourceTableName;
    }


    @Override
    protected void compute() {
        if (end - start > CodisConfiguration.getInt(CodisConfiguration.CODIS_IMPORT_THRESHOLD, CodisConfiguration.CODIS_IMPORT_THRESHOLD_DEFAULT)) {
            int mid = (end + start) / 2;

            ClientToCodisHelper left = new ClientToCodisHelper(dataList, codisHash, sourceTableName, jedisPool, start, mid);

            ClientToCodisHelper right = new ClientToCodisHelper(dataList, codisHash, sourceTableName, jedisPool, mid + 1, end);

            this.invokeAll(left, right);

        } else {
            Jedis jedis;
            try {
                jedis = jedisPool.getResource();
            }catch (Exception e){
                logger.error("Can not get resource from JedisResourcePool.", e);
                return;
            }
            //Jedis jedis = jedisPool.getResource();
            Pipeline pipeline = jedis.pipelined();
            int brokenRowNum = 0;

            logger.debug("Start from " + start + " to " + end + " in " + sourceTableName);

            for (int i = start; i <= end; i++) {

                //row的末尾如果是有空值，例如,,切分之后不能忽略空值
                String[] row = dataList.get(i).split(CodisConfiguration.DEFAULT_SEPARATOR, -1);

                if (row.length != codisHash.getSourceTableSchema().get(sourceTableName).size()) {
                    logger.warn("The row<" + dataList.get(i) + "> is invalid.");
                    logger.warn("The row length is " + row.length + " and schema is <" + codisHash.getSourceTableSchema().get(sourceTableName) + "> from table " + sourceTableName);
                    brokenRowNum++;
                    continue;
                }

                Assembly assembly;
                try {
                    Class newoneClass = null;
                    if (StringUtils.isNotEmpty(codisHash.getHandlerClass())) {
                        newoneClass = Class.forName(codisHash.getHandlerClass());
                    } else {
                        if (codisHash.getForeignKeys().length == 1) {
                            newoneClass = Class.forName(CodisHash.SIGLE_FOREIGN_KEY_HADLE_CLASS);
                        } else if (codisHash.getForeignKeys().length > 1) {
                            newoneClass = Class.forName(CodisHash.MULTI_FOREIGN_KEY_HADLE_CLASS);
                        } else {
                            logger.error("Can not determine handle class");
                            System.exit(9);
                        }
                    }

                    assembly = (Assembly) newoneClass.newInstance();


                    if (assembly != null && assembly.execute(codisHash, sourceTableName, row)) {
                        Map<String, Map<String, String>> hmset = assembly.getHmset();

                        for (Map.Entry<String, Map<String, String>> entry : hmset.entrySet()) {
                            String hmsetKey = entry.getKey();
                            Map<String, String> hmsetValue = entry.getValue();
                            jedis.hmset(hmsetKey, hmsetValue);
                        }

                    } else {
                        logger.error("Unknown error, please check schema configuration.");
                    }

                } catch (Exception e) {
                    logger.error(e);
                }

            }// end of for

            logger.debug("There are " + (end - start - brokenRowNum + 1) + " rows had been sent to codis.");


            pipeline.syncAndReturnAll();
            jedis.close();
        }
    }
}
