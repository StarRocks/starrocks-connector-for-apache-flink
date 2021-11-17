// StarRocks
create database flink_source;

CREATE TABLE flink_type_test (
    date_1 date,
    datetime_1 datetime,
    char_1 char(20),
    varchar_1 varchar(20),
    boolean_1 boolean,
    tinyint_1 tinyint,
    smallint_1 smallint,
    int_1 int,
    bigint_1 bigint,
    largeint_1 largeint,
    float_1 float,
    double_1 double,
    decimal_1 decimal(27,9)
)
PARTITION BY RANGE(date_1)
(
  PARTITION p1 VALUES LESS THAN ('2020-01-01'),
  PARTITION p2 VALUES LESS THAN ('2020-02-01'),
  PARTITION p3 VALUES LESS THAN ('2020-03-01')
)
DISTRIBUTED BY HASH(`tinyint_1`) BUCKETS 10
properties (
  "replication_num" = "2"
);

insert into flink_type_test (
    date_1,datetime_1,char_1,varchar_1,boolean_1,tinyint_1,smallint_1,int_1,bigint_1,largeint_1,float_1,double_1,decimal_1
    ) values (
    '2020-01-23','2020-01-23 00:00:00.1','C','B',1,0,-32768,-2147483648,-9223372036854775808,-18446744073709551616,-3.1,-3.14,-3.1);

insert into flink_type_test (
    date_1,datetime_1,char_1,varchar_1,boolean_1,tinyint_1,smallint_1,int_1,bigint_1,largeint_1,float_1,double_1,decimal_1
    ) values (
    '2020-02-23','2020-02-23 00:00:00.12','A','C',1,127,-32768,-2147483648,-9223372036854775808,-18446744073709551616,-3.1,-3.14,-3.14);

insert into flink_type_test (
    date_1,datetime_1,char_1,varchar_1,boolean_1,tinyint_1,smallint_1,int_1,bigint_1,largeint_1,float_1,double_1,decimal_1
    ) values (
    '2020-02-23','2020-02-23 00:00:00.123','A','C',1,-128,-32768,-2147483648,-9223372036854775808,-18446744073709551616,-3.1,-3.14,-3.141);

insert into flink_type_test (
    date_1,datetime_1,char_1,varchar_1,boolean_1,tinyint_1,smallint_1,int_1,bigint_1,largeint_1,float_1,double_1,decimal_1
    ) values (
    '2019-03-23','2020-03-23 00:00:00.1897','D','C',1,-128,-32768,-2147483648,-9223372036854775808,-18446744073709551616,-3.1,-3.14,-3.1412);

insert into flink_type_test (
    date_1,datetime_1,char_1,varchar_1,boolean_1,tinyint_1,smallint_1,int_1,bigint_1,largeint_1,float_1,double_1,decimal_1
    ) values (
    '2019-03-23','2020-03-23 00:00:00.18975','B','C',1,-128,-32768,-2147483648,-9223372036854775808,-18446744073709551616,-3.1,-3.14,-3.14129);

insert into flink_type_test (
    date_1,datetime_1,char_1,varchar_1,boolean_1,tinyint_1,smallint_1,int_1,bigint_1,largeint_1,float_1,double_1,decimal_1
    ) values (
    '2019-03-23','2020-03-23 00:00:00.18976','C','A',1,0,-32768,-2147483648,-9223372036854775808,-18446744073709551616,-3.1,-3.14,-3.141291);



// Flink
CREATE TABLE flink_type_test (
    date_1 DATE,
    datetime_1 TIMESTAMP(6),
    char_1 CHAR(20),
    varchar_1 VARCHAR,
    boolean_1 BOOLEAN,
    tinyint_1 TINYINT,
    smallint_1 SMALLINT,
    int_1 INT,
    bigint_1 BIGINT,
    largeint_1 STRING,
    float_1 FLOAT,
    double_1 DOUBLE,
    decimal_1 DECIMAL(27,9)
) WITH (
  'connector'='starrocks',
  'scan-url'='172.26.92.152:8634',
  'jdbc-url'='jdbc:mysql://172.26.92.152:9632',
  'username'='root',
  'password'='',
  'database-name'='flink_source',
  'table-name'='flink_type_test'
);