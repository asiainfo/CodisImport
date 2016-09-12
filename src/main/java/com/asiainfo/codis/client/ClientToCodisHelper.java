package com.asiainfo.codis.client;

import com.asiainfo.codis.actions.Assembly;
import com.asiainfo.codis.schema.CodisHash;
import com.asiainfo.codis.schema.DataSchema;
import com.asiainfo.conf.CodisConfiguration;
import com.asiainfo.util.CommonUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import redis.clients.jedis.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.RecursiveAction;

/**
 * Created by peng on 16/8/31.
 */
public class ClientToCodisHelper extends RecursiveAction {
    private Logger logger = Logger.getLogger(ClientToCodisHelper.class);

    private ShardedJedisPool jedisPool;
    private List<String> dataList;

    private CodisHash codisHash;
    private String sourceTableName;

    private int start;
    private int end;

    public ClientToCodisHelper(List<String> dataList, CodisHash codisHash, String sourceTableName, ShardedJedisPool jedisPool, int start, int end) {
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

        }
        else {
            ShardedJedis shardedJedis = jedisPool.getResource();
            ShardedJedisPipeline pipeline = shardedJedis.pipelined();
            int brokenRowNum = 0;

            for (int i = start; i <= end; i++) {
                String[] row = dataList.get(i).split(CodisConfiguration.DEFAULT_SEPARATOR);

                if (row.length != codisHash.getSourceTableSchema().get(sourceTableName).size()){
                    logger.warn("The row<" + dataList.get(i) + "> is invalid.");
                    brokenRowNum++;
                    continue;
                }

                Assembly assembly = null;
                try {
                    Class newoneClass = null;
                    if (StringUtils.isNotEmpty(codisHash.getHandlerClass())){
                        newoneClass = Class.forName(codisHash.getHandlerClass());
                    }else {
                        if (codisHash.getForeignKeys().length == 1){
                            newoneClass = Class.forName(CodisHash.SIGLE_FOREIGN_KEY_HADLE_CLASS);
                        }else if (codisHash.getForeignKeys().length > 1){
                            newoneClass = Class.forName(CodisHash.MULTI_FOREIGN_KEY_HADLE_CLASS);
                        }else {
                            logger.error("Can not determine handle class");
                            System.exit(9);
                        }
                    }

                                        assembly = (Assembly) newoneClass.newInstance();
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                } catch (InstantiationException e) {
                    e.printStackTrace();
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                }

                if (assembly != null && assembly.execute(codisHash, sourceTableName, row)){
                    shardedJedis.hmset(assembly.getKey(), assembly.getMap());
                }
                else {
                    logger.error("Unknown error, please check schema.json.");
                }

            }

            logger.debug("There are " + (end - start - brokenRowNum + 1) + " rows had been sent to codis.");

            pipeline.syncAndReturnAll();
            shardedJedis.close();
        }
    }
}
