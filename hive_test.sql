show databases;
-- 如果表存在删除表
DROP TABLE IF EXISTS test;
create table test
(
    name    string,
    friends array<string>,
    chilren map<string,int>,
    address struct<street:string, city:string>
)
--  列分隔符
    row format delimited fields terminated by ','
        --  在hive中，map、array、struct都使用collection items terminated by来指定，所以只能共用一个分隔符
--  MAP STRUCT 和 ARRAY 的分隔符(数据分割符号)
        collection items terminated by '_'
--  MAP 中的 key 与 value 的分隔符
        map keys terminated by ':'
    LOCATION '/warehouse/hive_test/test';

--  向表中装载数据（Load），追加输入，local : 从本地加载数据到hive，不写则从hdfs路径加载数据到hive
load data local inpath '/opt/file/test.txt' into table test;
--  数据装载，覆盖输入
load data local inpath '/opt/file/test.txt' overwrite into table test;

-- 查询语句中创建表并加载数据（As Select）
create table if not exists test1 location '/warehouse/hive_test/test1'
as
select name, friends
from test;

-- 将查询的结果导出到本地
insert overwrite local directory
    '/opt/file/test1'
select *
from test1;

-- Truncate 只能清空内部表数据，不能清空外部表中数据
truncate table test1;

-- 创建部门表
create table if not exists dept
(
    deptno int,
    dname  string,
    loc    int
)
    row format delimited fields terminated by '\t'
    location '/warehouse/hive_test/dept';

-- 数据装载
load data local inpath '/opt/file/hive_test_data/dept.txt' into table dept;

-- 创建员工表
create table if not exists emp
(
    empno    int,
    ename    string,
    job      string,
    mgr      int,
    hiredate string,
    sal      double,
    comm     double,
    deptno   int
)
    row format delimited fields terminated by '\t'
    location '/warehouse/hive_test/emp';

-- 数据装载
load data local inpath '/opt/file/hive_test_data/emp.txt' into table emp;

-- 创建一级分区表
create table dept_partition
(
    deptno int,
    dname  string,
    loc    string
)
    partitioned by (day string)
    row format delimited fields terminated by '\t'
    location '/warehouse/hive_test/dept_partition';

-- 数据装载
load data local inpath '/opt/file/hive_test_data/dept_2020-04-01.log' overwrite into table dept_partition partition (day = '2020-04-01');
load data local inpath '/opt/file/hive_test_data/dept_2020-04-02.log' overwrite into table dept_partition partition (day = '2020-04-02');
load data local inpath '/opt/file/hive_test_data/dept_2020-04-03.log' overwrite into table dept_partition partition (day = '2020-04-03');

-- 增加分区
alter table dept_partition
    add partition (day = '2020-04-08');
alter table dept_partition
    add partition (day = '2020-04-05') partition (day = '2020-04-06');

-- 动态分区，将最后一个字段作为分区字段，方案一
insert into table dept_partition partition (day)
select deptno, dname, loc, '2020-04-09'
from dept_partition;

-- 动态分区，将最后一个字段作为分区字段，方案二 (hive 3.0之后才能)
insert into table dept_partition
select deptno, dname, loc, '2020-04-9'
from dept_partition;

-- 删除分区
alter table dept_partition
    drop partition (day = '2020-04-010');

-- 查看表分区
show partitions dept_partition;

-- 修复分区元数据
msck repair table dept_partition;

-- 创建多级分区表
create table dept_partition2
(
    deptno int,
    dname  string,
    loc    string
)
    partitioned by (day string,hour string)
    row format delimited fields terminated by '\t'
    location '/warehouse/hive_test/dept_partition2';

load data local inpath '/opt/file/hive_test_data/dept_2020-04-03.log' into table dept_partition2 partition (day = '2020-04-01',hour = '12');

-- 创建分桶表
create table stu_buck
(
    id   int,
    name string
)
--  分桶字段必须为表字段的某一个，分桶个数为4
    clustered by (id) into 4 buckets
    row format delimited fields terminated by '\t'
    location '/warehouse/hive_test/stu_buck';

-- 数据加载
-- 相同的 id 会被分到同一个桶
load data local inpath '/opt/file/hive_test_data/stu_buck.txt' into table stu_buck;
insert into table stu_buck
select *
from stu_buck;

-- 分桶表抽样查询
select *
-- 将数据分成4份，从第1份开始抽
from stu_buck tablesample (bucket 1 out of 4 on id);

-- 查看系统自带的函数
show functions;
-- 显示自带的函数的用法
desc function max;
-- 详细显示自带的函数的用法
desc function extended ntile;

-- 查询：如果员工的 comm 为 NULL，则用-1 代替
-- nvl(a,b) : 给值为 NULL 的数据赋值，如果a为NULL，则NVL函数返回b的值，否则返回a的值
select comm, nvl(comm, -1)
from emp;

create table emp_sex
(
    name    string,
    dept_id string,
    sex     string
)
    row format delimited fields terminated by "\t"
    location '/warehouse/hive_test/emp_sex';

load data local inpath "/opt/file/hive_test_data/emp_sex.txt" into table emp_sex;

-- 需求求出不同部门男女各多少人，方案一
select dept_id,
       sum(case sex when '男' then 1 else 0 end) male_count,
       sum(case sex when '女' then 1 else 0 end) female_count
from emp_sex
group by dept_id;

-- 需求求出不同部门男女各多少人，方案二
select dept_id,
       sum(`if`(sex = '男', 1, 0)) male_count,
       sum(`if`(sex = '女', 1, 0)) female_count
from emp_sex
group by dept_id;

create table person_info
(
    name          string,
    constellation string,
    blood_type    string
)
    row format delimited fields terminated by "\t"
    location "/warehouse/hive_test/person_info";

load data local inpath "/opt/file/hive_test_data/person_info.txt" into table person_info;

-- 需求:把星座和血型一样的人归类到一起
select t1.c_b,
       concat_ws("|", collect_set(t1.name))
from (
         select name,
                concat_ws(",", constellation, blood_type) c_b
         from person_info
     ) t1
group by t1.c_b;

create table movie_info
(
    movie    string,
    category string
)
    row format delimited fields terminated by "\t"
    location "/warehouse/hive_test/movie_info";

load data local inpath "/opt/file/hive_test_data/movie_info.txt" into table movie_info;

-- 需求：将电影分类中的数组数据展开，方案一
select movie,
       tmp.category
from (
         select movie,
                split(category, ",") category
         from movie_info
     ) mi lateral view explode(mi.category) tmp as category;

-- 需求：将电影分类中的数组数据展开，方案二
select movie,
       category_name
-- explode() : 炸裂函数
-- lateral view : 用于和 split, explode 等 UDTF 一起使用，将炸开的列进行横向填充
from movie_info lateral view explode(split(category, ",")) movie_info_tmp as category_name;

create table business
(
    name      string,
    orderdate string,
    cost      int
)
    row format delimited fields terminated by ","
    location "/warehouse/hive_test/business";

load data local inpath "/opt/file/hive_test_data/business.txt" into table business;

-- 需求1：查询在2017年4月份购买过的顾客及总人数，方案一
select b1.name, b2.num
from (select distinct(name) name
      from business
      where substring(orderdate, 1, 7) = "2017-04") b1
         join
     (select count(distinct (name)) num
      from business
      where substring(orderdate, 1, 7) = "2017-04") b2;
-- 方案二
-- over() : 不写条件则默认使用全表数据
select name, count(*) over ()
from business
where substring(orderdate, 1, 7) = "2017-04"
group by name;

-- 需求2：查询顾客的购买明细及月购买总额
-- year() : 只求当前年   month() : 只求当前月   day() : 求当月第几天（几号）     weekofyear() : 求当前是一年的第几周
-- datediff(date1,date2) : 求时间间差值
select name, orderdate, cost, sum(cost) over (partition by month(orderdate))
from business;

-- 需求3：查看顾客上次的购买时间
-- lag() : 返回前 1 行的数据，为 null 设置为 1970-01-01     lead() : 返回后 1 行的数据，为 null 设置为 2000-01-01
select name,
       orderdate,
       cost,
       lag(orderdate, 1, "1970-01-01") over (partition by name order by orderdate) time1
from business;

-- 需求4：将每个顾客的 cost 按照日期进行累加，方案一
select name,
       orderdate,
       cost,
       sum(cost) over (partition by name order by orderdate rows between unbounded preceding and current row )
from business;
-- 方案二
select name,
       orderdate,
       cost,
       sum(cost) over (partition by name order by orderdate)
from business;

-- 需求5：将每个顾客的 cost 按照当前日期的前后进行累加
select name,
       orderdate,
       cost,
       sum(cost) over (partition by name order by orderdate rows between 1 preceding and 1 following)
from business;

-- 需求6：查询前 20%时间的订单信息
select *
from (
         select name,
                orderdate,
                cost,
--              ntile() : 等频分箱，把有序窗口的行分发到指定数据的组中，各个组有编号，编号从 1 开始，对于每一行，NTILE 返回此行所属的组的编号
                ntile(5) over (order by orderdate) sorted
         from business
     ) t
where sorted = 1;

create table score
(
    name    string,
    subject string,
    score   int
)
    row format delimited fields terminated by "\t"
    location "/warehouse/hive_test/score";

load data local inpath "/opt/file/hive_test_data/score.txt" overwrite into table score;

-- 需求1：计算每门学科成绩排名
select name,
       subject,
       score,
       rank() over (partition by subject order by score desc )      rp,
       dense_rank() over (partition by subject order by score desc) drp,
       row_number() over (partition by subject order by score desc) rmp
from score;

-- 需求2：求出每门学科前三名的学生
select *
from (select name,
             subject,
             score,
             rank() over (partition by subject order by score desc )      rp,
             dense_rank() over (partition by subject order by score desc) drp,
             row_number() over (partition by subject order by score desc) rmp
      from score) s
where s.rmp <= 3;

-- 创建表，存储数据格式为ORC
create table log_orc
(
    track_time  string,
    url         string,
    session_id  string,
    referer     string,
    ip          string,
    end_user_id string,
    city_id     string
)
    row format delimited fields terminated by "\t"
--  数据存储格式为 orc
    stored as orc
    location "/warehouse/hive_test/log_orc"
--  设置orc存储不使用压缩
    tblproperties ("orc.compress" = "none");

-- 创建表，存储数据格式为parquet
create table log_parquet
(
    track_time  string,
    url         string,
    session_id  string,
    referer     string,
    ip          string,
    end_user_id string,
    city_id     string
)
    row format delimited fields terminated by "\t"
--  数据存储格式为 parquet
    stored as parquet
    location "/warehouse/hive_test/log_parquet";

-- 创建一个 ZLIB 压缩的 ORC 存储方式
create table log_orc_zlib
(
    track_time  string,
    url         string,
    session_id  string,
    referer     string,
    ip          string,
    end_user_id string,
    city_id     string
)
    row format delimited fields terminated by '\t'
--  数据存储格式为 orc
    stored as orc
    location "/warehouse/hive_test/log_orc_zlib"
--  设置 orc 存储的压缩为 ZLIB
    tblproperties ("orc.compress" = "ZLIB");

-- 创建一个 SNAPPY 压缩的 ORC 存储方式
create table log_orc_zlib
(
    track_time  string,
    url         string,
    session_id  string,
    referer     string,
    ip          string,
    end_user_id string,
    city_id     string
)
    row format delimited fields terminated by '\t'
--  数据存储格式为 orc
    stored as orc
    location "/warehouse/hive_test/log_orc_zlib"
--  设置 orc 存储的压缩为 snappy
    tblproperties ("orc.compress" = "SNAPPY");

-- 创建一个 SNAPPY 压缩的 parquet 存储方式
create table log_parquet_snappy
(
    track_time  string,
    url         string,
    session_id  string,
    referer     string,
    ip          string,
    end_user_id string,
    city_id     string
)
    row format delimited fields terminated by '\t'
    stored as parquet
    location "/warehouse/hive_test/log_parquet_snappy"
    tblproperties ("parquet.compression" = "SNAPPY");

-- 创建原始数据表：gulivideo_ori
create table gulivideo_ori
(
    videoId   string,
    uploader  string,
    age       int,
    category  array<string>,
    length    int,
    views     int,
    rate      float,
    ratings   int,
    comments  int,
    relatedId array<string>
)
    row format delimited fields terminated by "\t"
        collection items terminated by "&"
    location "/warehouse/hive_test/gulivideo_ori";

-- 数据装载
load data local inpath "/opt/file/hive_test_data/video/*" into table gulivideo_ori;

-- 创建原始数据表: gulivideo_user_ori
create table gulivideo_user_ori
(
    uploader string,
    videos   int,
    friends  int
)
    row format delimited fields terminated by "\t"
    location "/warehouse/hive_test/gulivideo_user_ori";

-- 数据装载
load data local inpath "/opt/file/hive_test_data/user/*" into table gulivideo_user_ori;

-- 创建 orc 存储格式带 snappy 压缩的表
create table gulivideo_orc
(
    videoId   string,
    uploader  string,
    age       int,
    category  array<string>,
    length    int,
    views     int,
    rate      float,
    ratings   int,
    comments  int,
    relatedId array<string>
)
    stored as orc
    location "/warehouse/hive_test/gulivideo_orc"
    tblproperties ("orc.compress" = "SNAPPY");

-- 数据装载
insert into table gulivideo_orc
select *
from gulivideo_ori;

-- 创建 orc 存储格式带 snappy 压缩的表
create table gulivideo_user_orc
(
    uploader string,
    videos   int,
    friends  int
)
    row format delimited fields terminated by "\t"
    stored as orc
    location "/warehouse/hive_test/gulivideo_user_orc"
    tblproperties ("orc.compress" = "SNAPPY");

-- 数据装载
insert into table gulivideo_user_orc
select *
from gulivideo_user_ori;

-- 需求1：统计视频观看数 Top10
select videoId,
       `views`
from gulivideo_orc
order by `views` desc
limit 10;

-- 需求2：统计视频类别热度(每个类别下的视频数) Top10，方案一
SELECT t1.category_name,
       COUNT(t1.videoId) hot
FROM (
         SELECT videoId,
                category_name
         FROM gulivideo_orc
                  lateral VIEW explode(category) gulivideo_orc_tmp AS category_name
     ) t1
GROUP BY t1.category_name
ORDER BY hot DESC
LIMIT 10;
-- 方案二
select gulivideo_orc_tmp.categroy_name,
       count(videoId) count_videoId
from gulivideo_orc lateral view explode(category) gulivideo_orc_tmp as categroy_name
group by gulivideo_orc_tmp.categroy_name
order by count_videoId desc
limit 10;

-- 需求3：统计出视频观看数最高的 20 个视频的所属类别以及类别包含 Top20 视频的个数
-- 1、求出视频观看数最高的20个视频的所属类别
select category
from gulivideo_orc
order by `views` desc
limit 20;
-- 2、将所属类别炸开
select explode(category) category_name
from (
         select category
         from gulivideo_orc
         order by `views` desc
         limit 20
     ) t1;
-- 3、最终sql，将炸开的类别分组后count(*)
select category_name,
       count(*)
from (
         select explode(category) category_name
         from (
                  select category
                  from gulivideo_orc
                  order by `views` desc
                  limit 20
              ) t1
     ) t2
group by category_name;

-- 需求4：统计视频观看数 Top50 所关联视频的所属类别排序
-- 1、求出视频观看数最高的50个视频的相关视频Id
select relatedId
from gulivideo_orc
order by `views` desc
limit 50;
-- 2、将相关视频Id炸开
select explode(relatedId)
from (select relatedId
      from gulivideo_orc
      order by `views` desc
      limit 50) t1;
-- 3、将炸开后的视频Id和原表join，找到所属类别
select explode(category) category_name
from (select explode(relatedId) videoId
      from (select relatedId
            from gulivideo_orc
            order by `views` desc
            limit 50) go) t1
         join gulivideo_orc t2 on t1.videoId = t2.videoId;
-- 4、最终sql，将炸开的类别分组后count()
select category_name,
       count(category_name)                             num,
       rank() over (order by count(category_name) desc) rk
from (
         select explode(category) category_name
         from (
                  select explode(relatedId) videoId
                  from (
                           select relatedId
                           from gulivideo_orc
                           order by `views` desc
                           limit 50) t1) t2
                  join gulivideo_orc t3 on t2.videoId = t3.videoId
     ) t4
group by category_name
order by num desc;

-- 需求5：统计每个类别中的视频热度 Top10，以 Music 为例
SELECT videoId,
       `views`,
       category_name
FROM gulivideo_orc lateral VIEW explode(category) gulivideo_orc_tmp AS category_name
WHERE category_name = "Music"
ORDER BY `views` DESC
LIMIT 10;

-- 需求6：统计每个类别视频观看数 Top10
-- 1、求出每个类别视频观看数排名
select category_name,
       videoId,
       `views`,
       rank() over (partition by category_name order by `views` desc) rk
from gulivideo_orc lateral view explode(category) gulivideo_orc_tmp AS category_name;

-- 组内操作用 相应的函数 + over() 开窗

-- 2、取出每个类别视频观看数前10名
select category_name,
       videoId,
       `views`
from (
         select category_name,
                videoId,
                `views`,
                rank() over (partition by category_name order by `views` desc) rk
         from gulivideo_orc lateral view explode(category) gulivideo_orc_tmp AS category_name) t1
where rk <= 10;

-- 需求7：统计上传视频最多的用户 Top10 以及他们上传的视频观看次数在前 20 的视频
-- 1、求出上传视频最多的用户10个用户
select uploader,
       videos
from gulivideo_user_orc
order by videos desc
limit 10;
-- 2、上传视频最多的用户10个用户与视频表join并安照uploader组内排序
select t1.uploader,
       t2.videoId,
       t2.`views`,
       rank() over (partition by t1.uploader order by t2.`views`) rk
from (
         select uploader,
                videos
         from gulivideo_user_orc
         order by videos desc
         limit 10
     ) t1
         join gulivideo_orc t2 on t1.uploader = t2.uploader;
-- 3、最终sql，取出每组内排名前20的数据
select uploader,
       videoId,
       `views`,
       rk
from (select t1.uploader,
             t2.videoId,
             t2.`views`,
             rank() over (partition by t1.uploader order by t2.`views` desc) rk
      from (
               select uploader,
                      videos
               from gulivideo_user_orc
               order by videos desc
               limit 10
           ) t1
               join gulivideo_orc t2 on t1.uploader = t2.uploader) t2
where rk <= 20;

create table spark_hive_test
(
    name string,
    age  int,
    gpa  float
)
    row format delimited fields terminated by "\t"
    location "/warehouse/hive_test/spark_hive_test";

-- 建立 Hive 表，关联 HBase 不存在的表，插入数据到 Hive 表的同时能够影响 HBase 表
CREATE TABLE hive_hbase_emp_table
(
    empno    int,
    ename    string,
    job      string,
    mgr      int,
    hiredate string,
    sal      double,
    comm     double,
    deptno   int
)
--  hive和hbase整合指定处理的存储器
    STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
--      指定Hbase表和hive的字段映射关系
        WITH SERDEPROPERTIES ("hbase.columns.mapping" =
            ":key,info:ename,info:job,info:mgr,info:hiredate,info:sal,info:comm,info:deptno")
--  设置在hbase中的表名
    TBLPROPERTIES ("hbase.table.name" = "bigdata:hbase_emp_table");

-- 在 Hive 中创建临时中间表，用于 load 文件中的数据，不能将数据直接 load 进 Hive 所关联 HBase 的那张表中
CREATE TABLE hive_emp_hbase
(
    empno    int,
    ename    string,
    job      string,
    mgr      int,
    hiredate string,
    sal      double,
    comm     double,
    deptno   int
)
    row format delimited fields terminated by '\t'
    location "/warehouse/hive_test/hive_emp_hbase";

load data local inpath "/opt/file/hive_emp_hbase.txt" into table hive_emp_hbase;

insert into table hive_hbase_emp_table
select *
from hive_emp_hbase;

select deptno, avg(sal) monery
from hive_hbase_emp_table
group by deptno;

-- 在 Hive 中创建外部表关联 hbase 中已经存在的表
CREATE EXTERNAL TABLE relevance_hbase_emp
(
    empno    int,
    ename    string,
    job      string,
    mgr      int,
    hiredate string,
    sal      double,
    comm     double,
    deptno   int
)
    STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
        WITH SERDEPROPERTIES ("hbase.columns.mapping" =
            ":key,info:ename,info:job,info:mgr,info:hiredate,info:sal,info:comm,info:deptno")
    TBLPROPERTIES ("hbase.table.name" = "hbase_emp_table");







