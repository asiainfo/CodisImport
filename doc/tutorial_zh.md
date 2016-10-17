CodisImport
==================

简介
--------
CodisImport是用来将外部结构化数据以Hash的形式导入codis或者redis。用户可以通过配置外部结构化数据的schema，指定导入之后
Hash的key以及所需要的field。

配置
--------
主要有两个配置文件codis.properties和schema.json

###codis.properties

| Property | Description | 
|:----|:----|
|codis.address|目标codis或者redis的地址，多个地址之间用逗号分隔。例如host1:6379,host2:19000注意这里要使用机器的hostname，不要用ip|
|codis.import.threshold|每个线程要处理数据源的行数，默认10000|
|codis.client.thread-count|线程池启用的最大线程数，默认为机器的cpu核数|
|codis.client.liveness-monitor.expiry-interval-ms|监控线程完成情况的时间间隔，默认10000ms|
|codis.input.file.path|外部数据存放路径|
|split.file.enable|是否将外部数据切分处理，此功能是针对一个输入源文件太大，为提高效率切分成小文件并行处理，默认false，不开启此功能|
|codis.maximum-operation-byte|每个切分文件的大小，仅当split.file.enable为true时生效|
|zk.address|jodis用来连接zk的地址|
|zk.session.timeout-ms|jodis连接zk的超时时间|
|zk.proxy.dir|codis的proxy在zk中的存储路径，jodis创建连接时候使用|

###schema.json
此配置文件为json格式，以下面的配置为例说明一下主要的配置项

例如有一个外部表叫做TD_IMSI_SEGMENT_ATTR

| imsi | city_code |city_name|
|:----:|:----:|:----:|
|469527|029|西安|
|469528|010|北京|

要将这个表导入到redis里面，其中Hash到key为`city_info:469527`和`city_info:469528`，也就是key到前缀是city_info，
后缀为表TD_IMSI_SEGMENT_ATTR中到主键到值。如果通过客户端到命令行到形式导入，则为
```bash
HMSET city_info:469527 city_code "029" city_name "西安"
HMSET city_info:469528 city_code "010" city_name "北京"
```
可以看到，数据源表中到每一行转换为了redis中的一个Hash，Hash的key就是数据源表的主键加一个前缀，数据源表的其它字段转换为了Hash的field。

```json
[
  {
    "keyPrefix": "city_info",
    "foreignKeys": ["imsi"],
    "sourceTableSchema": {
      "TD_IMSI_SEGMENT_ATTR": ["city_code","city_name"]
    },
    "hashFields": ["city_code","city_name"]
  }
]
```
那么现在看这个配置就非常清楚了，`keyPrefix`为要生产的Hash的key。`foreignKeys`为外部表的主键，这里可以写多个，用逗号分隔。
`sourceTableSchema`外部数据源的表的schema，要保证这里指定的字段顺序和数据源的顺序一致。`sourceTableSchema`里面可以配置多个表，但是必须保证
每个表都是以`foreignKeys`中所指定的字段为主键的。`hashFields`为要从数据源中导入的做为Hash中field的字段，这些字段一定要在`sourceTableSchema`中
可以找到。cd 

使用方法
--------
