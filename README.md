## MyDatabase
Forked from [shardingsphere-workshop](https://github.com/shardingsphere-workshop/workshop20200415)

## 项目说明
1. 项目结构：
&nbsp;&nbsp;execute-engine:&nbsp;&nbsp;执行引擎，负责sql的执行计划
&nbsp;&nbsp;mysql-protocol:&nbsp;&nbsp;mysql通信协议api
&nbsp;&nbsp;mysql-proxy:&nbsp;&nbsp;mysql基于netty实现mysql代理，负责与mysql client交互
&nbsp;&nbsp;sharding-parser:&nbsp;&nbsp;sql语法解析
&nbsp;&nbsp;storage-engine-api:&nbsp;&nbsp;存储引擎接口层
&nbsp;&nbsp;storage-engine-bplus:&nbsp;&nbsp;基于B+树索引实现的存储引擎
&nbsp;&nbsp;storage-engine-lsm:&nbsp;&nbsp;基于lsm原理实现的存储引擎
2. 启动类：mysql-proxy模块shardingsphere.workshop.mysql.proxy.Bootstrap
3. 操作示例：
```cmd
mysql -uroot -proot
CREATE TABLE IF NOT EXISTS t_order(id INT UNSIGNED AUTO_INCREMENT,name VARCHAR(100) NOT NULL, creator VARCHAR(40) NOT NULL, PRIMARY KEY ( id ));

INSERT INTO t_order ( id, name, creator ) VALUES ( 1, 'n1', 'c1' );
INSERT INTO t_order ( id, name, creator ) VALUES ( 2, 'n2’, 'c2’ );
INSERT INTO t_order ( id, name, creator ) VALUES ( 3, 'n3’, 'c3’ );

select * from t_order where id > 1
select * from t_order where id = 1
select * from t_order where id < 3
select id, name from t_order where id = 1
```

## 完成情况
1. 基本dml、ddl sql解析
2. 支持创建表、插入、查询等操作
3. 支持B+树和LSM两种存储引擎

## 后续计划

1. 复杂sql支持（join、group by、order by）等
2. sql执行优化
3. 并发及事务支持
4. 分布式数据库支持