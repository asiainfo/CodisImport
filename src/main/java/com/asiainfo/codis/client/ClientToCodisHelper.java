package com.asiainfo.codis.client;

import com.asiainfo.codis.schema.DataSchema;
import com.asiainfo.conf.CodisConfiguration;
import org.apache.log4j.Logger;
import redis.clients.jedis.*;

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
    private DataSchema schema;

    private int start;
    private int end;

    public ClientToCodisHelper(List<String> dataList, DataSchema schema, ShardedJedisPool jedisPool, int start, int end) {
        this.dataList = dataList;
        this.schema = schema;
        this.jedisPool = jedisPool;
        this.start = start;
        this.end = end;
    }


    @Override
    protected void compute() {
        if (end - start > CodisConfiguration.getInt(CodisConfiguration.CODIS_IMPORT_THRESHOLD, CodisConfiguration.CODIS_IMPORT_THRESHOLD_DEFAULT)) {
            int mid = (end + start) / 2;

            ClientToCodisHelper left = new ClientToCodisHelper(dataList, schema, jedisPool, start, mid);

            ClientToCodisHelper right = new ClientToCodisHelper(dataList, schema, jedisPool, mid + 1, end);

            this.invokeAll(left, right);

        }
        else {
            ShardedJedis shardedJedis = jedisPool.getResource();
            ShardedJedisPipeline pipeline = shardedJedis.pipelined();
            int brokenRowNum = 0;

            for (int i = start; i <= end; i++) {
                String[] rows = dataList.get(i).split(CodisConfiguration.DEFAULT_SEPARATOR);

                String redisKey = schema.getTableName() + ":" + rows[schema.getTablePrimaryKeyIndex()].trim();
                HashMap<String, String> values = new HashMap();

                if (rows.length != schema.getHeader().length){
                    logger.warn("The row<" + dataList.get(i) + "> is invalid.");
                    brokenRowNum++;
                    continue;
                }

                for (int j = 0; j < schema.getHeader().length; j++){
                    values.put(schema.getHeader()[j].trim(), rows[j].trim());
                }

                shardedJedis.hmset(redisKey, values);
            }

            logger.debug("There are " + (end - start - brokenRowNum + 1) + " rows had been sent to codis.");

            pipeline.syncAndReturnAll();
            shardedJedis.close();
        }
    }
}
