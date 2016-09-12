package com.asiainfo.codis.client;

import com.asiainfo.codis.schema.CodisHash;
import com.asiainfo.codis.schema.DataSchema;
import com.asiainfo.conf.CodisConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.PrefixFileFilter;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.log4j.Logger;
import redis.clients.jedis.*;
import redis.clients.util.Hashing;
import redis.clients.util.Sharded;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;

/**
 * Created by peng on 16/8/30.
 */
public class ClientToCodis {
    private Logger logger = Logger.getLogger(ClientToCodis.class);
    private JedisPoolConfig config = new JedisPoolConfig();

    //private List<DataSchema> schemaList;
    private List<CodisHash> codisHashList;
    private String inputDataPath;

    public ClientToCodis(List<CodisHash> codisHashList, String inputDataPath) {
        this.codisHashList = codisHashList;
        this.inputDataPath = inputDataPath;
        init();
    }

    public void sendData() {
        if (!new File(inputDataPath).exists()) {
            logger.error("Can not find any input data since <" + inputDataPath + "> did not exist.");
            return;
        }
        long startTime = System.currentTimeMillis();
        List<JedisShardInfo> shards = new ArrayList();

        List<String> addressList = CodisConfiguration.getStringArray(CodisConfiguration.CODIS_ADDRESS, CodisConfiguration.DEFAULT_SEPARATOR);

        for (String address : addressList) {
            String[] _address = StringUtils.splitByWholeSeparator(StringUtils.trimToEmpty(address), ":");
            if (_address.length != 2) {
                logger.error("The address<" + address + "> is invalid.");
                continue;
            }

            shards.add(new JedisShardInfo(_address[0], NumberUtils.toInt(_address[1])));
        }

        ShardedJedisPool pool = new ShardedJedisPool(config, shards);

        ForkJoinPool fjpool = new ForkJoinPool(CodisConfiguration.getInt(CodisConfiguration.CODIS_CLIENT_THREAD_COUNT, CodisConfiguration.DEFAULT_CODIS_CLIENT_THREAD_COUNT));

        try {

            //=========
            File dir = FileUtils.getFile(inputDataPath);
            for (CodisHash codisHash : codisHashList) {

                for (String tableName : codisHash.getSourceTableSchema().keySet()) {

                    for (String fileName : dir.list(new PrefixFileFilter(tableName))) {
                        List<String> dataList = FileUtils.readLines(new File(inputDataPath + File.separator + fileName), "UTF-8");
                        fjpool.execute(new ClientToCodisHelper(dataList, codisHash, tableName, pool, 0, dataList.size() - 1));
                    }
                }

            }

            //=======

            fjpool.shutdown();

            while (!fjpool.awaitTermination(CodisConfiguration.getLong(CodisConfiguration.CODIS_CLIENT_LIVENESS_MONITOR_EXPIRY_INTERVAL_MS, CodisConfiguration.DEFAULT_CODIS_CLIENT_LIVENESS_MONITOR_EXPIRY_INTERVAL_MS), TimeUnit.MILLISECONDS)) {
                logger.debug("There are <" + fjpool.getParallelism() + "> threads running at the same time.");
                logger.info("Left <" + (fjpool.getRunningThreadCount()) + "> running thread.");
            }

            logger.info("Exit since no running thread.");
        } catch (InterruptedException e) {
            logger.error(e);
        } catch (IOException e) {
            logger.error(e);
        }


        pool.close();

        long endTime = System.currentTimeMillis();

        logger.info("Take " + (endTime - startTime) + " ms");
    }


    private void init() {
        config = new JedisPoolConfig();
        //连接耗尽时是否阻塞, false报异常,ture阻塞直到超时, 默认true
        config.setBlockWhenExhausted(true);
        //设置的逐出策略类名, 默认DefaultEvictionPolicy(当连接超过最大空闲时间,或连接数超过最大空闲连接数)
        config.setEvictionPolicyClassName("org.apache.commons.pool2.impl.DefaultEvictionPolicy");
        //是否启用pool的jmx管理功能, 默认true
        config.setJmxEnabled(true);
        //MBean ObjectName = new ObjectName("org.apache.commons.pool2:type=GenericObjectPool,name=" + "pool" + i); 默 认为"pool", JMX不熟,具体不知道是干啥的...默认就好.
        config.setJmxNamePrefix("pool");
        //是否启用后进先出, 默认true
        config.setLifo(true);
        //最大空闲连接数, 默认8个
        config.setMaxIdle(100);
        //最大连接数, 默认8个
        config.setMaxTotal(100);
        //获取连接时的最大等待毫秒数(如果设置为阻塞时BlockWhenExhausted),如果超时就抛异常, 小于零:阻塞不确定的时间,  默认-1
        config.setMaxWaitMillis(-1);
        //逐出连接的最小空闲时间 默认1800000毫秒(30分钟)
        config.setMinEvictableIdleTimeMillis(1800000);
        //最小空闲连接数, 默认0
        config.setMinIdle(0);
        //每次逐出检查时 逐出的最大数目 如果为负数就是 : 1/abs(n), 默认3
        config.setNumTestsPerEvictionRun(3);
        //对象空闲多久后逐出, 当空闲时间>该值 且 空闲连接>最大空闲数 时直接逐出,不再根据MinEvictableIdleTimeMillis判断  (默认逐出策略)
        config.setSoftMinEvictableIdleTimeMillis(1800000);
        //在获取连接的时候检查有效性, 默认false
        config.setTestOnBorrow(false);
        //在空闲时检查有效性, 默认false
        config.setTestWhileIdle(false);
        //逐出扫描的时间间隔(毫秒) 如果为负数,则不运行逐出线程, 默认-1
        config.setTimeBetweenEvictionRunsMillis(-1);
    }

}
