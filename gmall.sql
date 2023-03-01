create database gmall;

use gmall;

-- {"id":1001,"name":"zhangsan"}
-- 基于json格式创建表,字段名与json字符串的字段名一致，否则无法导入数据
create table person
(
    id   int,
    name string
)
-- 行格式序列
    row format serde 'org.apache.hadoop.hive.serde2.JsonSerDe'
-- 保存在hdfs的地址
    location '/person';

select *
from person;

/*
ODS层的设计要点如下:
（1）ODS层的表结构设计依托于从业务系统同步过来的数据结构
（2）ODS层要保存全部历史数据，故其压缩格式应选择压缩比较高的，此处选择gzip
（3）ODS层表名的命名规范为：ods_表名_单分区增量全量标识（inc/full）
*/

-- 如果表存在删除表
DROP TABLE IF EXISTS ods_log_inc;

-- 基于json格式创建表,字段名与json字符串的字段名一致，否则无法导入数据
-- 日志表    EXTERNAL : 外部表   COMMENT : 注释   BIGINT : 整数类型，强类型，相当于java的long类型
CREATE EXTERNAL TABLE ods_log_inc
(
    `common`   STRUCT<ar :STRING,ba :STRING,ch :STRING,is_new :STRING,md :STRING,mid :STRING,os :STRING,uid :STRING,vc
                      :STRING> COMMENT '公共信息',
    `page`     STRUCT<during_time :STRING,item :STRING,item_type :STRING,last_page_id :STRING,page_id :STRING,source_type
                      :STRING> COMMENT '页面信息',
    `actions`  ARRAY<STRUCT<action_id:STRING,item:STRING,item_type:STRING,ts:BIGINT>> COMMENT '动作信息',
    `displays` ARRAY<STRUCT<display_type :STRING,item :STRING,item_type :STRING,`order` :STRING,pos_id
                            :STRING>> COMMENT '曝光信息',
    `start`    STRUCT<entry :STRING,loading_time :BIGINT,open_ad_id :BIGINT,open_ad_ms :BIGINT,open_ad_skip_ms
                      :BIGINT> COMMENT '启动信息',
    `err`      STRUCT<error_code:BIGINT,msg:STRING> COMMENT '错误信息',
    `ts`       BIGINT COMMENT '时间戳'
) COMMENT '活动信息表'
--  创建分区表，按照dt字段分区，分区字段不能是表中已经存在的数据，可以将分区字段看作表的伪列
    PARTITIONED BY (`dt` STRING)
--  行格式序列(序列化和反序列化)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
--  保存在hdfs的地址
    LOCATION '/warehouse/gmall/ods/ods_log_inc/';

-- 修复表分区数据
msck repair table ods_log_inc;

--  向表中装载数据（Load），将hdfs路径中的数据写入hive表中，追加写入，相当于剪切，分区表加载数据时，必须指定分区
load data inpath '/origin_data/gmall/log/topic_log/2020-06-14' into table ods_log_inc partition (dt = '2020-06-14');

-- 显示分区
show partitions ods_log_inc;

-- 活动信息表（全量表）表结构跟mysql的保持一致
CREATE EXTERNAL TABLE ods_activity_info_full
(
    `id`            STRING COMMENT '活动id',
    `activity_name` STRING COMMENT '活动名称',
    `activity_type` STRING COMMENT '活动类型',
    `activity_desc` STRING COMMENT '活动描述',
    `start_time`    STRING COMMENT '开始时间',
    `end_time`      STRING COMMENT '结束时间',
    `create_time`   STRING COMMENT '创建时间'
) COMMENT '活动信息表'
--  创建分区表，按照dt字段分区，分区字段和表中字段不能重复
    PARTITIONED BY (`dt` STRING)
--  每一行字段的分隔符为 \t
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
--  将null值定义为空字符串
        NULL DEFINED AS ''
--  保存在hdfs的地址
    LOCATION '/warehouse/gmall/ods/ods_activity_info_full/';

-- 购物车表（增量表）
CREATE EXTERNAL TABLE ods_cart_info_inc
(
    `type` STRING COMMENT '变动类型',
    `ts`   BIGINT COMMENT '变动时间',
    `data` STRUCT<id :STRING,user_id :STRING,sku_id :STRING,cart_price :DECIMAL(16, 2),sku_num :BIGINT,img_url :STRING,sku_name
                  :STRING,is_checked :STRING,create_time :STRING,operate_time :STRING,is_ordered :STRING,order_time
                  :STRING,source_type :STRING,source_id :STRING> COMMENT '数据',
    `old`  MAP<STRING,STRING> COMMENT '旧值'
) COMMENT '购物车增量表'
--  创建分区表，按照dt字段分区，分区字段和表中字段不能重复
    PARTITIONED BY (`dt` STRING)
--  行格式序列(序列化和反序列化)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
--  保存在hdfs的地址
    LOCATION '/warehouse/gmall/ods/ods_cart_info_inc/';

-- 活动规则表（全量表）
CREATE EXTERNAL TABLE ods_activity_rule_full
(
    `id`               STRING COMMENT '编号',
    `activity_id`      STRING COMMENT '类型',
    `activity_type`    STRING COMMENT '活动类型',
    `condition_amount` DECIMAL(16, 2) COMMENT '满减金额',
    `condition_num`    BIGINT COMMENT '满减件数',
    `benefit_amount`   DECIMAL(16, 2) COMMENT '优惠金额',
    `benefit_discount` DECIMAL(16, 2) COMMENT '优惠折扣',
    `benefit_level`    STRING COMMENT '优惠级别'
) COMMENT '活动规则表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        NULL DEFINED AS ''
    LOCATION '/warehouse/gmall/ods/ods_activity_rule_full/';

-- 一级品类表（全量表）
CREATE EXTERNAL TABLE ods_base_category1_full
(
    `id`   STRING COMMENT '编号',
    `name` STRING COMMENT '分类名称'
) COMMENT '一级品类表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        NULL DEFINED AS ''
    LOCATION '/warehouse/gmall/ods/ods_base_category1_full/';

-- 二级品类表（全量表）
CREATE EXTERNAL TABLE ods_base_category2_full
(
    `id`           STRING COMMENT '编号',
    `name`         STRING COMMENT '二级分类名称',
    `category1_id` STRING COMMENT '一级分类编号'
) COMMENT '二级品类表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        NULL DEFINED AS ''
    LOCATION '/warehouse/gmall/ods/ods_base_category2_full/';

-- 三级品类表（全量表）
CREATE EXTERNAL TABLE ods_base_category3_full
(
    `id`           STRING COMMENT '编号',
    `name`         STRING COMMENT '三级分类名称',
    `category2_id` STRING COMMENT '二级分类编号'
) COMMENT '三级品类表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        NULL DEFINED AS ''
    LOCATION '/warehouse/gmall/ods/ods_base_category3_full/';

-- 编码字典表（全量表）
CREATE EXTERNAL TABLE ods_base_dic_full
(
    `dic_code`     STRING COMMENT '编号',
    `dic_name`     STRING COMMENT '编码名称',
    `parent_code`  STRING COMMENT '父编号',
    `create_time`  STRING COMMENT '创建日期',
    `operate_time` STRING COMMENT '修改日期'
) COMMENT '编码字典表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        NULL DEFINED AS ''
    LOCATION '/warehouse/gmall/ods/ods_base_dic_full/';

-- 省份表（全量表）
CREATE EXTERNAL TABLE ods_base_province_full
(
    `id`         STRING COMMENT '编号',
    `name`       STRING COMMENT '省份名称',
    `region_id`  STRING COMMENT '地区ID',
    `area_code`  STRING COMMENT '地区编码',
    `iso_code`   STRING COMMENT '旧版ISO-3166-2编码，供可视化使用',
    `iso_3166_2` STRING COMMENT '新版IOS-3166-2编码，供可视化使用'
) COMMENT '省份表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        NULL DEFINED AS ''
    LOCATION '/warehouse/gmall/ods/ods_base_province_full/';

-- 地区表（全量表）
CREATE EXTERNAL TABLE ods_base_region_full
(
    `id`          STRING COMMENT '编号',
    `region_name` STRING COMMENT '地区名称'
) COMMENT '地区表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        NULL DEFINED AS ''
    LOCATION '/warehouse/gmall/ods/ods_base_region_full/';

-- 品牌表（全量表）
CREATE EXTERNAL TABLE ods_base_trademark_full
(
    `id`       STRING COMMENT '编号',
    `tm_name`  STRING COMMENT '品牌名称',
    `logo_url` STRING COMMENT '品牌logo的图片路径'
) COMMENT '品牌表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        NULL DEFINED AS ''
    LOCATION '/warehouse/gmall/ods/ods_base_trademark_full/';

-- 购物车表（全量表）
CREATE EXTERNAL TABLE ods_cart_info_full
(
    `id`           STRING COMMENT '编号',
    `user_id`      STRING COMMENT '用户id',
    `sku_id`       STRING COMMENT 'sku_id',
    `cart_price`   DECIMAL(16, 2) COMMENT '放入购物车时价格',
    `sku_num`      BIGINT COMMENT '数量',
    `img_url`      BIGINT COMMENT '商品图片地址',
    `sku_name`     STRING COMMENT 'sku名称 (冗余)',
    `is_checked`   STRING COMMENT '是否被选中',
    `create_time`  STRING COMMENT '创建时间',
    `operate_time` STRING COMMENT '修改时间',
    `is_ordered`   STRING COMMENT '是否已经下单',
    `order_time`   STRING COMMENT '下单时间',
    `source_type`  STRING COMMENT '来源类型',
    `source_id`    STRING COMMENT '来源编号'
) COMMENT '购物车全量表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        NULL DEFINED AS ''
    LOCATION '/warehouse/gmall/ods/ods_cart_info_full/';

-- 优惠券信息表（全量表）
CREATE EXTERNAL TABLE ods_coupon_info_full
(
    `id`               STRING COMMENT '购物券编号',
    `coupon_name`      STRING COMMENT '购物券名称',
    `coupon_type`      STRING COMMENT '购物券类型 1 现金券 2 折扣券 3 满减券 4 满件打折券',
    `condition_amount` DECIMAL(16, 2) COMMENT '满额数',
    `condition_num`    BIGINT COMMENT '满件数',
    `activity_id`      STRING COMMENT '活动编号',
    `benefit_amount`   DECIMAL(16, 2) COMMENT '减金额',
    `benefit_discount` DECIMAL(16, 2) COMMENT '折扣',
    `create_time`      STRING COMMENT '创建时间',
    `range_type`       STRING COMMENT '范围类型 1、商品 2、品类 3、品牌',
    `limit_num`        BIGINT COMMENT '最多领用次数',
    `taken_count`      BIGINT COMMENT '已领用次数',
    `start_time`       STRING COMMENT '开始领取时间',
    `end_time`         STRING COMMENT '结束领取时间',
    `operate_time`     STRING COMMENT '修改时间',
    `expire_time`      STRING COMMENT '过期时间'
) COMMENT '优惠券信息表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        NULL DEFINED AS ''
    LOCATION '/warehouse/gmall/ods/ods_coupon_info_full/';

-- 商品平台属性表（全量表）
CREATE EXTERNAL TABLE ods_sku_attr_value_full
(
    `id`         STRING COMMENT '编号',
    `attr_id`    STRING COMMENT '平台属性ID',
    `value_id`   STRING COMMENT '平台属性值ID',
    `sku_id`     STRING COMMENT '商品ID',
    `attr_name`  STRING COMMENT '平台属性名称',
    `value_name` STRING COMMENT '平台属性值名称'
) COMMENT 'sku平台属性表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        NULL DEFINED AS ''
    LOCATION '/warehouse/gmall/ods/ods_sku_attr_value_full/';

-- 商品表（全量表）
CREATE EXTERNAL TABLE ods_sku_info_full
(
    `id`              STRING COMMENT 'skuId',
    `spu_id`          STRING COMMENT 'spuid',
    `price`           DECIMAL(16, 2) COMMENT '价格',
    `sku_name`        STRING COMMENT '商品名称',
    `sku_desc`        STRING COMMENT '商品描述',
    `weight`          DECIMAL(16, 2) COMMENT '重量',
    `tm_id`           STRING COMMENT '品牌id',
    `category3_id`    STRING COMMENT '品类id',
    `sku_default_igm` STRING COMMENT '商品图片地址',
    `is_sale`         STRING COMMENT '是否在售',
    `create_time`     STRING COMMENT '创建时间'
) COMMENT 'SKU商品表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        NULL DEFINED AS ''
    LOCATION '/warehouse/gmall/ods/ods_sku_info_full/';

-- 商品销售属性值表（全量表）
CREATE EXTERNAL TABLE ods_sku_sale_attr_value_full
(
    `id`                   STRING COMMENT '编号',
    `sku_id`               STRING COMMENT 'sku_id',
    `spu_id`               STRING COMMENT 'spu_id',
    `sale_attr_value_id`   STRING COMMENT '销售属性值id',
    `sale_attr_id`         STRING COMMENT '销售属性id',
    `sale_attr_name`       STRING COMMENT '销售属性名称',
    `sale_attr_value_name` STRING COMMENT '销售属性值名称'
) COMMENT 'sku销售属性名称'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        NULL DEFINED AS ''
    LOCATION '/warehouse/gmall/ods/ods_sku_sale_attr_value_full/';

-- SPU表（全量表）
CREATE EXTERNAL TABLE ods_spu_info_full
(
    `id`           STRING COMMENT 'spu_id',
    `spu_name`     STRING COMMENT 'spu名称',
    `description`  STRING COMMENT '描述信息',
    `category3_id` STRING COMMENT '品类id',
    `tm_id`        STRING COMMENT '品牌id'
) COMMENT 'SPU商品表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        NULL DEFINED AS ''
    LOCATION '/warehouse/gmall/ods/ods_spu_info_full/';

-- 评论表（增量表）
CREATE EXTERNAL TABLE ods_comment_info_inc
(
    `type` STRING COMMENT '变动类型',
    `ts`   BIGINT COMMENT '变动时间',
    `data` STRUCT<id :STRING,user_id :STRING,nick_name :STRING,head_img :STRING,sku_id :STRING,spu_id :STRING,order_id
                  :STRING,appraise :STRING,comment_txt :STRING,create_time :STRING,operate_time :STRING> COMMENT '数据',
    `old`  MAP<STRING,STRING> COMMENT '旧值'
) COMMENT '评价表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
    LOCATION '/warehouse/gmall/ods/ods_comment_info_inc/';

-- 优惠券领用表（增量表）
CREATE EXTERNAL TABLE ods_coupon_use_inc
(
    `type` STRING COMMENT '变动类型',
    `ts`   BIGINT COMMENT '变动时间',
    `data` STRUCT<id :STRING,coupon_id :STRING,user_id :STRING,order_id :STRING,coupon_status :STRING,get_time :STRING,using_time
                  :STRING,used_time :STRING,expire_time :STRING> COMMENT '数据',
    `old`  MAP<STRING,STRING> COMMENT '旧值'
) COMMENT '优惠券领用表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
    LOCATION '/warehouse/gmall/ods/ods_coupon_use_inc/';

-- 收藏表（增量表）
CREATE EXTERNAL TABLE ods_favor_info_inc
(
    `type` STRING COMMENT '变动类型',
    `ts`   BIGINT COMMENT '变动时间',
    `data` STRUCT<id :STRING,user_id :STRING,sku_id :STRING,spu_id :STRING,is_cancel :STRING,create_time :STRING,cancel_time
                  :STRING> COMMENT '数据',
    `old`  MAP<STRING,STRING> COMMENT '旧值'
) COMMENT '收藏表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
    LOCATION '/warehouse/gmall/ods/ods_favor_info_inc/';

-- 订单明细表（增量表）
CREATE EXTERNAL TABLE ods_order_detail_inc
(
    `type` STRING COMMENT '变动类型',
    `ts`   BIGINT COMMENT '变动时间',
    `data` STRUCT<id :STRING,order_id :STRING,sku_id :STRING,sku_name :STRING,img_url :STRING,order_price
                  :DECIMAL(16, 2),sku_num :BIGINT,create_time :STRING,source_type :STRING,source_id :STRING,split_total_amount
                  :DECIMAL(16, 2),split_activity_amount :DECIMAL(16, 2),split_coupon_amount
                  :DECIMAL(16, 2)> COMMENT '数据',
    `old`  MAP<STRING,STRING> COMMENT '旧值'
) COMMENT '订单明细表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
    LOCATION '/warehouse/gmall/ods/ods_order_detail_inc/';

-- 订单明细活动关联表（增量表）
CREATE EXTERNAL TABLE ods_order_detail_activity_inc
(
    `type` STRING COMMENT '变动类型',
    `ts`   BIGINT COMMENT '变动时间',
    `data` STRUCT<id :STRING,order_id :STRING,order_detail_id :STRING,activity_id :STRING,activity_rule_id :STRING,sku_id
                  :STRING,create_time :STRING> COMMENT '数据',
    `old`  MAP<STRING,STRING> COMMENT '旧值'
) COMMENT '订单明细活动关联表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
    LOCATION '/warehouse/gmall/ods/ods_order_detail_activity_inc/';

-- 订单明细优惠券关联表（增量表）
CREATE EXTERNAL TABLE ods_order_detail_coupon_inc
(
    `type` STRING COMMENT '变动类型',
    `ts`   BIGINT COMMENT '变动时间',
    `data` STRUCT<id :STRING,order_id :STRING,order_detail_id :STRING,coupon_id :STRING,coupon_use_id :STRING,sku_id
                  :STRING,create_time :STRING> COMMENT '数据',
    `old`  MAP<STRING,STRING> COMMENT '旧值'
) COMMENT '订单明细优惠券关联表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
    LOCATION '/warehouse/gmall/ods/ods_order_detail_coupon_inc/';

-- 订单表（增量表）
CREATE EXTERNAL TABLE ods_order_info_inc
(
    `type` STRING COMMENT '变动类型',
    `ts`   BIGINT COMMENT '变动时间',
    `data` STRUCT<id :STRING,consignee :STRING,consignee_tel :STRING,total_amount :DECIMAL(16, 2),order_status :STRING,user_id
                  :STRING,payment_way :STRING,delivery_address :STRING,order_comment :STRING,out_trade_no :STRING,trade_body
                  :STRING,create_time :STRING,operate_time :STRING,expire_time :STRING,process_status :STRING,tracking_no
                  :STRING,parent_order_id :STRING,img_url :STRING,province_id :STRING,activity_reduce_amount
                  :DECIMAL(16, 2),coupon_reduce_amount :DECIMAL(16, 2),original_total_amount :DECIMAL(16, 2),freight_fee
                  :DECIMAL(16, 2),freight_fee_reduce :DECIMAL(16, 2),refundable_time :DECIMAL(16, 2)> COMMENT '数据',
    `old`  MAP<STRING,STRING> COMMENT '旧值'
) COMMENT '订单表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
    LOCATION '/warehouse/gmall/ods/ods_order_info_inc/';

-- 退单表（增量表）
CREATE EXTERNAL TABLE ods_order_refund_info_inc
(
    `type` STRING COMMENT '变动类型',
    `ts`   BIGINT COMMENT '变动时间',
    `data` STRUCT<id :STRING,user_id :STRING,order_id :STRING,sku_id :STRING,refund_type :STRING,refund_num :BIGINT,refund_amount
                  :DECIMAL(16, 2),refund_reason_type :STRING,refund_reason_txt :STRING,refund_status :STRING,create_time
                  :STRING> COMMENT '数据',
    `old`  MAP<STRING,STRING> COMMENT '旧值'
) COMMENT '退单表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
    LOCATION '/warehouse/gmall/ods/ods_order_refund_info_inc/';

-- 订单状态流水表（增量表）
CREATE EXTERNAL TABLE ods_order_status_log_inc
(
    `type` STRING COMMENT '变动类型',
    `ts`   BIGINT COMMENT '变动时间',
    `data` STRUCT<id :STRING,order_id :STRING,order_status :STRING,operate_time :STRING> COMMENT '数据',
    `old`  MAP<STRING,STRING> COMMENT '旧值'
) COMMENT '退单表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
    LOCATION '/warehouse/gmall/ods/ods_order_status_log_inc/';

-- 支付表（增量表）
CREATE EXTERNAL TABLE ods_payment_info_inc
(
    `type` STRING COMMENT '变动类型',
    `ts`   BIGINT COMMENT '变动时间',
    `data` STRUCT<id :STRING,out_trade_no :STRING,order_id :STRING,user_id :STRING,payment_type :STRING,trade_no
                  :STRING,total_amount :DECIMAL(16, 2),subject :STRING,payment_status :STRING,create_time :STRING,callback_time
                  :STRING,callback_content :STRING> COMMENT '数据',
    `old`  MAP<STRING,STRING> COMMENT '旧值'
) COMMENT '支付表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
    LOCATION '/warehouse/gmall/ods/ods_payment_info_inc/';

-- 退款表（增量表）
CREATE EXTERNAL TABLE ods_refund_payment_inc
(
    `type` STRING COMMENT '变动类型',
    `ts`   BIGINT COMMENT '变动时间',
    `data` STRUCT<id :STRING,out_trade_no :STRING,order_id :STRING,sku_id :STRING,payment_type :STRING,trade_no :STRING,total_amount
                  :DECIMAL(16, 2),subject :STRING,refund_status :STRING,create_time :STRING,callback_time :STRING,callback_content
                  :STRING> COMMENT '数据',
    `old`  MAP<STRING,STRING> COMMENT '旧值'
) COMMENT '退款表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
    LOCATION '/warehouse/gmall/ods/ods_refund_payment_inc/';

-- 用户表（增量表）
CREATE EXTERNAL TABLE ods_user_info_inc
(
    `type` STRING COMMENT '变动类型',
    `ts`   BIGINT COMMENT '变动时间',
    `data` STRUCT<id :STRING,login_name :STRING,nick_name :STRING,passwd :STRING,name :STRING,phone_num :STRING,email
                  :STRING,head_img :STRING,user_level :STRING,birthday :STRING,gender :STRING,create_time :STRING,operate_time
                  :STRING,status :STRING> COMMENT '数据',
    `old`  MAP<STRING,STRING> COMMENT '旧值'
) COMMENT '用户表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
    LOCATION '/warehouse/gmall/ods/ods_user_info_inc/';

/*
DIM层设计要点:
（1）DIM层的设计依据是维度建模理论，该层存储维度模型的维度表，数据来源ODS层
（2）DIM层的数据存储格式为orc列式存储+snappy压缩。
（3）DIM层表名的命名规范为dim_表名_全量表或者拉链表标识（full/zip）
*/

-- 商品维度表    DECIMAL(16, 2) : 高精度浮点型，最多16位数字，后2位是小数
CREATE EXTERNAL TABLE dim_sku_full
(
    `id`                   STRING COMMENT 'sku_id',
    `price`                DECIMAL(16, 2) COMMENT '商品价格',
    `sku_name`             STRING COMMENT '商品名称',
    `sku_desc`             STRING COMMENT '商品描述',
    `weight`               DECIMAL(16, 2) COMMENT '重量',
    `is_sale`              BOOLEAN COMMENT '是否在售',
    `spu_id`               STRING COMMENT 'spu编号',
    `spu_name`             STRING COMMENT 'spu名称',
    `category3_id`         STRING COMMENT '三级分类id',
    `category3_name`       STRING COMMENT '三级分类名称',
    `category2_id`         STRING COMMENT '二级分类id',
    `category2_name`       STRING COMMENT '二级分类名称',
    `category1_id`         STRING COMMENT '一级分类id',
    `category1_name`       STRING COMMENT '一级分类名称',
    `tm_id`                STRING COMMENT '品牌id',
    `tm_name`              STRING COMMENT '品牌名称',
    `sku_attr_values`      ARRAY<STRUCT<attr_id :STRING,value_id :STRING,attr_name :STRING,value_name
                                        :STRING>> COMMENT '平台属性',
    `sku_sale_attr_values` ARRAY<STRUCT<sale_attr_id :STRING,sale_attr_value_id :STRING,sale_attr_name :STRING,sale_attr_value_name
                                        :STRING>> COMMENT '销售属性',
    `create_time`          STRING COMMENT '创建时间'
) COMMENT '商品维度表'
--  创建分区表，按照dt字段分区，分区字段和表中字段不能重复
    PARTITIONED BY (`dt` STRING)
--  设置数据存储格式为ORC
    STORED AS ORC
--  保存在hdfs的地址
    LOCATION '/warehouse/gmall/dim/dim_sku_full/'
--  设置 orc 存储的压缩为 snappy
    TBLPROPERTIES ('orc.compress' = 'snappy');

-- 数据装载(分段测试)，2020-06-14
select id,
       spu_id,
       price,
       sku_name,
       sku_desc,
       weight,
       tm_id,
       category3_id,
       is_sale,
       create_time
from ods_sku_info_full
where dt = '2020-06-14';

select id,
       spu_name
from ods_spu_info_full
where dt = '2020-06-14';

select id,
       name,
       category2_id
from ods_base_category3_full
where dt = '2020-06-14';

select id,
       name,
       category1_id
from ods_base_category2_full
where dt = '2020-06-14';

select id,
       name
from ods_base_category1_full
where dt = '2020-06-14';

select id,
       tm_name
from ods_base_trademark_full
where dt = '2020-06-14';

select sku_id,
       collect_set(
               named_struct("attr_id", attr_id, "value_id", value_id, "attr_name", attr_name, "value_name", value_name))
from ods_sku_attr_value_full
where dt = '2020-06-14'
group by sku_id;

select sku_id,
       collect_set(
               named_struct('sale_attr_id', sale_attr_id, 'sale_attr_value_id', sale_attr_value_id, 'sale_attr_name',
                            sale_attr_name, 'sale_attr_value_name', sale_attr_value_name)) sale_attrs
from ods_sku_sale_attr_value_full
where dt = '2020-06-14'
group by sku_id;

-- 数据装载
with sku as
         (
             select id,
                    price,
                    sku_name,
                    sku_desc,
                    weight,
                    is_sale,
                    spu_id,
                    category3_id,
                    tm_id,
                    create_time
             from ods_sku_info_full
             where dt = '2020-06-14'
         ),
     spu as
         (
             select id,
                    spu_name
             from ods_spu_info_full
             where dt = '2020-06-14'
         ),
     c3 as
         (
             select id,
                    name,
                    category2_id
             from ods_base_category3_full
             where dt = '2020-06-14'
         ),
     c2 as
         (
             select id,
                    name,
                    category1_id
             from ods_base_category2_full
             where dt = '2020-06-14'
         ),
     c1 as
         (
             select id,
                    name
             from ods_base_category1_full
             where dt = '2020-06-14'
         ),
     tm as
         (
             select id,
                    tm_name
             from ods_base_trademark_full
             where dt = '2020-06-14'
         ),
     attr as
         (
             select sku_id,
                    collect_set(
                            named_struct('attr_id', attr_id, 'value_id', value_id, 'attr_name', attr_name, 'value_name',
                                         value_name)) attrs
             from ods_sku_attr_value_full
             where dt = '2020-06-14'
             group by sku_id
         ),
     sale_attr as
         (
             select sku_id,
                    collect_set(named_struct('sale_attr_id', sale_attr_id, 'sale_attr_value_id', sale_attr_value_id,
                                             'sale_attr_name', sale_attr_name, 'sale_attr_value_name',
                                             sale_attr_value_name)) sale_attrs
             from ods_sku_sale_attr_value_full
             where dt = '2020-06-14'
             group by sku_id
         )
-- 通过查询语句向表中插入数据（insert），insert overwrite : 覆盖写入  insert into：以追加数据的方式插入到表或分区，原有数据不会删除
insert
overwrite
table
dim_sku_full
partition
(
dt = '2020-06-14'
)
select sku.id,
       sku.price,
       sku.sku_name,
       sku.sku_desc,
       sku.weight,
       sku.is_sale,
       sku.spu_id,
       spu.spu_name,
       sku.category3_id,
       c3.name,
       c3.category2_id,
       c2.name,
       c2.category1_id,
       c1.name,
       sku.tm_id,
       tm.tm_name,
       attr.attrs,
       sale_attr.sale_attrs,
       sku.create_time
from sku
         left join spu on sku.spu_id = spu.id
         left join c3 on sku.category3_id = c3.id
         left join c2 on c3.category2_id = c2.id
         left join c1 on c2.category1_id = c1.id
         left join tm on sku.tm_id = tm.id
         left join attr on sku.id = attr.sku_id
         left join sale_attr on sku.id = sale_attr.sku_id;

-- 优惠券维度表
CREATE EXTERNAL TABLE dim_coupon_full
(
    `id`               STRING COMMENT '购物券编号',
    `coupon_name`      STRING COMMENT '购物券名称',
    `coupon_type_code` STRING COMMENT '购物券类型编码',
    `coupon_type_name` STRING COMMENT '购物券类型名称',
    `condition_amount` DECIMAL(16, 2) COMMENT '满额数',
    `condition_num`    BIGINT COMMENT '满件数',
    `activity_id`      STRING COMMENT '活动编号',
    `benefit_amount`   DECIMAL(16, 2) COMMENT '减金额',
    `benefit_discount` DECIMAL(16, 2) COMMENT '折扣',
    `benefit_rule`     STRING COMMENT '优惠规则:满元*减*元，满*件打*折',
    `create_time`      STRING COMMENT '创建时间',
    `range_type_code`  STRING COMMENT '优惠范围类型编码',
    `range_type_name`  STRING COMMENT '优惠范围类型名称',
    `limit_num`        BIGINT COMMENT '最多领取次数',
    `taken_count`      BIGINT COMMENT '已领取次数',
    `start_time`       STRING COMMENT '可以领取的开始日期',
    `end_time`         STRING COMMENT '可以领取的结束日期',
    `operate_time`     STRING COMMENT '修改时间',
    `expire_time`      STRING COMMENT '过期时间'
) COMMENT '优惠券维度表'
--  创建分区表，按照dt字段分区，分区字段和表中字段不能重复
    PARTITIONED BY (`dt` STRING)
--  设置数据存储格式为ORC
    STORED AS ORC
--  保存在hdfs的地址
    LOCATION '/warehouse/gmall/dim/dim_coupon_full/'
--  设置 orc 存储的压缩为 snappy
    TBLPROPERTIES ('orc.compress' = 'snappy');

-- 数据装载
insert overwrite table dim_coupon_full partition (dt = '2020-06-14')
select id,
       coupon_name,
       coupon_type,
       coupon_dic.dic_name,
       condition_amount,
       condition_num,
       activity_id,
       benefit_amount,
       benefit_discount,
       case coupon_type
--         concat() : 字符串拼接
           when '3201' then concat('满', condition_amount, '元减', benefit_amount, '元')
           when '3202' then concat('满', condition_num, '件打', 10 * (1 - benefit_discount), '折')
           when '3203' then concat('减', benefit_amount, '元')
           end benefit_rule,
       create_time,
       range_type,
       range_dic.dic_name,
       limit_num,
       taken_count,
       start_time,
       end_time,
       operate_time,
       expire_time
from (
         select id,
                coupon_name,
                coupon_type,
                condition_amount,
                condition_num,
                activity_id,
                benefit_amount,
                benefit_discount,
                create_time,
                range_type,
                limit_num,
                taken_count,
                start_time,
                end_time,
                operate_time,
                expire_time
         from ods_coupon_info_full
         where dt = '2020-06-14'
     ) ci
         left join
     (
         select dic_code,
                dic_name
         from ods_base_dic_full
         where dt = '2020-06-14'
           and parent_code = '32'
     ) coupon_dic
     on ci.coupon_type = coupon_dic.dic_code
         left join
     (
         select dic_code,
                dic_name
         from ods_base_dic_full
         where dt = '2020-06-14'
           and parent_code = '33'
     ) range_dic
     on ci.range_type = range_dic.dic_code;

-- 活动维度表
CREATE EXTERNAL TABLE dim_activity_full
(
    `activity_rule_id`   STRING COMMENT '活动规则ID',
    `activity_id`        STRING COMMENT '活动ID',
    `activity_name`      STRING COMMENT '活动名称',
    `activity_type_code` STRING COMMENT '活动类型编码',
    `activity_type_name` STRING COMMENT '活动类型名称',
    `activity_desc`      STRING COMMENT '活动描述',
    `start_time`         STRING COMMENT '开始时间',
    `end_time`           STRING COMMENT '结束时间',
    `create_time`        STRING COMMENT '创建时间',
    `condition_amount`   DECIMAL(16, 2) COMMENT '满减金额',
    `condition_num`      BIGINT COMMENT '满减件数',
    `benefit_amount`     DECIMAL(16, 2) COMMENT '优惠金额',
    `benefit_discount`   DECIMAL(16, 2) COMMENT '优惠折扣',
    `benefit_rule`       STRING COMMENT '优惠规则',
    `benefit_level`      STRING COMMENT '优惠级别'
) COMMENT '活动信息表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dim/dim_activity_full/'
    TBLPROPERTIES ('orc.compress' = 'snappy');

-- 数据装载
insert overwrite table dim_activity_full partition (dt = '2020-06-14')
select rule.id,
       info.id,
       activity_name,
       rule.activity_type,
       dic.dic_name,
       activity_desc,
       start_time,
       end_time,
       create_time,
       condition_amount,
       condition_num,
       benefit_amount,
       benefit_discount,
       case rule.activity_type
           when '3101' then concat('满', condition_amount, '元减', benefit_amount, '元')
           when '3102' then concat('满', condition_num, '件打', 10 * (1 - benefit_discount), '折')
           when '3103' then concat('打', 10 * (1 - benefit_discount), '折')
           end benefit_rule,
       benefit_level
from (
         select id,
                activity_id,
                activity_type,
                condition_amount,
                condition_num,
                benefit_amount,
                benefit_discount,
                benefit_level
         from ods_activity_rule_full
         where dt = '2020-06-14'
     ) rule
         left join
     (
         select id,
                activity_name,
                activity_type,
                activity_desc,
                start_time,
                end_time,
                create_time
         from ods_activity_info_full
         where dt = '2020-06-14'
     ) info
     on rule.activity_id = info.id
         left join
     (
         select dic_code,
                dic_name
         from ods_base_dic_full
         where dt = '2020-06-14'
           and parent_code = '31'
     ) dic
     on rule.activity_type = dic.dic_code;

-- 地区维度表
CREATE EXTERNAL TABLE dim_province_full
(
    `id`            STRING COMMENT 'id',
    `province_name` STRING COMMENT '省市名称',
    `area_code`     STRING COMMENT '地区编码',
    `iso_code`      STRING COMMENT '旧版ISO-3166-2编码，供可视化使用',
    `iso_3166_2`    STRING COMMENT '新版IOS-3166-2编码，供可视化使用',
    `region_id`     STRING COMMENT '地区id',
    `region_name`   STRING COMMENT '地区名称'
) COMMENT '地区维度表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dim/dim_province_full/'
    TBLPROPERTIES ('orc.compress' = 'snappy');

-- 数据装载
insert overwrite table dim_province_full partition (dt = '2020-06-14')
select province.id,
       province.name,
       province.area_code,
       province.iso_code,
       province.iso_3166_2,
       region_id,
       region_name
from (
         select id,
                name,
                region_id,
                area_code,
                iso_code,
                iso_3166_2
         from ods_base_province_full
         where dt = '2020-06-14'
     ) province
         left join
     (
         select id,
                region_name
         from ods_base_region_full
         where dt = '2020-06-14'
     ) region
     on province.region_id = region.id;

-- 日期维度表
CREATE EXTERNAL TABLE dim_date
(
    `date_id`    STRING COMMENT '日期ID',
    `week_id`    STRING COMMENT '周ID,一年中的第几周',
    `week_day`   STRING COMMENT '周几',
    `day`        STRING COMMENT '每月的第几天',
    `month`      STRING COMMENT '一年中的第几月',
    `quarter`    STRING COMMENT '一年中的第几季度',
    `year`       STRING COMMENT '年份',
    `is_workday` STRING COMMENT '是否是工作日',
    `holiday_id` STRING COMMENT '节假日'
) COMMENT '时间维度表'
    STORED AS ORC
    LOCATION '/warehouse/gmall/dim/dim_date/'
    TBLPROPERTIES ('orc.compress' = 'snappy');

-- 日期维度过度表
CREATE EXTERNAL TABLE tmp_dim_date_info
(
    `date_id`    STRING COMMENT '日',
    `week_id`    STRING COMMENT '周ID',
    `week_day`   STRING COMMENT '周几',
    `day`        STRING COMMENT '每月的第几天',
    `month`      STRING COMMENT '第几月',
    `quarter`    STRING COMMENT '第几季度',
    `year`       STRING COMMENT '年',
    `is_workday` STRING COMMENT '是否是工作日',
    `holiday_id` STRING COMMENT '节假日'
) COMMENT '时间维度表'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/gmall/tmp/tmp_dim_date_info/';

-- 数据装载，先创建过度表，文本格式为textFile，将文件txt文件导入过度表，再将过度表导入目标表
insert overwrite table dim_date
select *
from tmp_dim_date_info;

-- 用户维度表，拉链表
CREATE EXTERNAL TABLE dim_user_zip
(
    `id`           STRING COMMENT '用户id',
    `login_name`   STRING COMMENT '用户名称',
    `nick_name`    STRING COMMENT '用户昵称',
    `name`         STRING COMMENT '用户姓名',
    `phone_num`    STRING COMMENT '手机号码',
    `email`        STRING COMMENT '邮箱',
    `user_level`   STRING COMMENT '用户等级',
    `birthday`     STRING COMMENT '生日',
    `gender`       STRING COMMENT '性别',
    `create_time`  STRING COMMENT '创建时间',
    `operate_time` STRING COMMENT '操作时间',
    `start_date`   STRING COMMENT '开始日期',
    `end_date`     STRING COMMENT '结束日期'
) COMMENT '用户表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dim/dim_user_zip/'
    TBLPROPERTIES ('orc.compress' = 'snappy');

-- 数据装载，首日装载    md5() : 加密函数
insert overwrite table dim_user_zip partition (dt = '9999-12-31')
select data.id,
       data.login_name,
       data.nick_name,
       md5(data.name),
       md5(data.phone_num),
       md5(data.email),
       data.user_level,
       data.birthday,
       data.gender,
       data.create_time,
       data.operate_time,
       '2020-06-14' start_date,
       '9999-12-31' end_date
from ods_user_info_inc
where dt = '2020-06-14'
  and type = 'bootstrap-insert';

-- 每日装载
-- 关闭动态分区严格模式，当前会话有效
set hive.exec.dynamic.partition.mode=nonstrict;
with tmp as
         (
             select old.id           old_id,
                    old.login_name   old_login_name,
                    old.nick_name    old_nick_name,
                    old.name         old_name,
                    old.phone_num    old_phone_num,
                    old.email        old_email,
                    old.user_level   old_user_level,
                    old.birthday     old_birthday,
                    old.gender       old_gender,
                    old.create_time  old_create_time,
                    old.operate_time old_operate_time,
                    old.start_date   old_start_date,
                    old.end_date     old_end_date,
                    new.id           new_id,
                    new.login_name   new_login_name,
                    new.nick_name    new_nick_name,
                    new.name         new_name,
                    new.phone_num    new_phone_num,
                    new.email        new_email,
                    new.user_level   new_user_level,
                    new.birthday     new_birthday,
                    new.gender       new_gender,
                    new.create_time  new_create_time,
                    new.operate_time new_operate_time,
                    new.start_date   new_start_date,
                    new.end_date     new_end_date
             from (
                      select id,
                             login_name,
                             nick_name,
                             name,
                             phone_num,
                             email,
                             user_level,
                             birthday,
                             gender,
                             create_time,
                             operate_time,
                             start_date,
                             end_date
                      from dim_user_zip
                      where dt = '9999-12-31'
                  ) old
                      full outer join
                  (
                      select id,
                             login_name,
                             nick_name,
                             md5(name)      name,
                             md5(phone_num) phone_num,
                             md5(email)     email,
                             user_level,
                             birthday,
                             gender,
                             create_time,
                             operate_time,
                             '2020-06-15'   start_date,
                             '9999-12-31'   end_date
                      from (
                               select data.id,
                                      data.login_name,
                                      data.nick_name,
                                      data.name,
                                      data.phone_num,
                                      data.email,
                                      data.user_level,
                                      data.birthday,
                                      data.gender,
                                      data.create_time,
                                      data.operate_time,
--                                    over() : 窗口函数(开窗函数),指定分析函数工作的数据窗口大小，这个数据窗口大小可能会随着行的变而变
--                                    partition by 分组，不改变数据行数，原来多少行还是多少行，group by 改变行数，只保留了group by 之后的结果
                                      row_number() over (partition by data.id order by ts desc) rn
                               from ods_user_info_inc
                               where dt = '2020-06-15'
                           ) t1
                      where rn = 1
                  ) new
                  on old.id = new.id
         )
insert
overwrite
table
dim_user_zip
partition
(
-- 动态分区，将下列两张表的最后一个字段作为分区字段
dt
)
select if(new_id is not null, new_id, old_id),
       if(new_id is not null, new_login_name, old_login_name),
       if(new_id is not null, new_nick_name, old_nick_name),
       if(new_id is not null, new_name, old_name),
       if(new_id is not null, new_phone_num, old_phone_num),
       if(new_id is not null, new_email, old_email),
       if(new_id is not null, new_user_level, old_user_level),
       if(new_id is not null, new_birthday, old_birthday),
       if(new_id is not null, new_gender, old_gender),
       if(new_id is not null, new_create_time, old_create_time),
       if(new_id is not null, new_operate_time, old_operate_time),
       if(new_id is not null, new_start_date, old_start_date),
       if(new_id is not null, new_end_date, old_end_date),
       if(new_id is not null, new_end_date, old_end_date) dt
from tmp
union all
select old_id,
       old_login_name,
       old_nick_name,
       old_name,
       old_phone_num,
       old_email,
       old_user_level,
       old_birthday,
       old_gender,
       old_create_time,
       old_operate_time,
       old_start_date,
--     date_add()  日期函数,向日期添加指定的时间间隔
       cast(date_add('2020-06-15', -1) as string) old_end_date,
       cast(date_add('2020-06-15', -1) as string) dt
from tmp
where old_id is not null
  and new_id is not null;

/*
DWD层设计要点:
（1）DWD层的设计依据是维度建模理论，该层存储维度模型的事实表，数据来源ODS层
（2）DWD层的数据存储格式为orc列式存储+snappy压缩。
（3）DWD层表名的命名规范为dwd_数据域_表名_单分区增量全量标识（inc/full）
*/

-- 交易域加购事务事实表
CREATE EXTERNAL TABLE dwd_trade_cart_add_inc
(
    `id`               STRING COMMENT '编号',
    `user_id`          STRING COMMENT '用户id',
    `sku_id`           STRING COMMENT '商品id',
    `date_id`          STRING COMMENT '时间id',
    `create_time`      STRING COMMENT '加购时间',
    `source_id`        STRING COMMENT '来源类型ID',
    `source_type_code` STRING COMMENT '来源类型编码',
    `source_type_name` STRING COMMENT '来源类型名称',
    `sku_num`          BIGINT COMMENT '加购物车件数'
) COMMENT '交易域加购物车事务事实表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS ORC
    LOCATION '/warehouse/gmall/dwd/dwd_trade_cart_add_inc/'
    TBLPROPERTIES ('orc.compress' = 'snappy');

-- 首日装载
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd_trade_cart_add_inc partition (dt)
select id,
       user_id,
       sku_id,
--     date_format() : 以不同的格式显示日期/时间数据
       date_format(create_time, 'yyyy-MM-dd') date_id,
       create_time,
       source_id,
       source_type,
       dic.dic_name,
       sku_num,
       date_format(create_time, 'yyyy-MM-dd')
from (
         select data.id,
                data.user_id,
                data.sku_id,
                data.create_time,
                data.source_id,
                data.source_type,
                data.sku_num
         from ods_cart_info_inc
         where dt = '2020-06-14'
           and type = 'bootstrap-insert'
     ) ci
         left join
     (
         select dic_code,
                dic_name
         from ods_base_dic_full
         where dt = '2020-06-14'
           and parent_code = '24'
     ) dic
     on ci.source_type = dic.dic_code;

-- 每日装载
insert overwrite table dwd_trade_cart_add_inc partition (dt = '2020-06-15')
select id,
       user_id,
       sku_id,
       date_id,
       create_time,
       source_id,
       source_type_code,
       source_type_name,
       sku_num
from (
         select data.id,
                data.user_id,
                data.sku_id,
--              from_utc_timestamp() : 转换时间地区
                date_format(from_utc_timestamp(ts * 1000, 'GMT+8'), 'yyyy-MM-dd')          date_id,
                date_format(from_utc_timestamp(ts * 1000, 'GMT+8'), 'yyyy-MM-dd HH:mm:ss') create_time,
                data.source_id,
                data.source_type                                                           source_type_code,
                if(type = 'insert', data.sku_num, data.sku_num - old['sku_num'])           sku_num
         from ods_cart_info_inc
         where dt = '2020-06-15'
           and (type = 'insert'
             or (type = 'update' and old['sku_num'] is not null and data.sku_num > cast(old['sku_num'] as int)))
     ) cart
         left join
     (
         select dic_code,
                dic_name source_type_name
         from ods_base_dic_full
         where dt = '2020-06-15'
           and parent_code = '24'
     ) dic
     on cart.source_type_code = dic.dic_code;

-- 交易域下单事务事实表
CREATE EXTERNAL TABLE dwd_trade_order_detail_inc
(
    `id`                    STRING COMMENT '编号',
    `order_id`              STRING COMMENT '订单id',
    `user_id`               STRING COMMENT '用户id',
    `sku_id`                STRING COMMENT '商品id',
    `province_id`           STRING COMMENT '省份id',
    `activity_id`           STRING COMMENT '参与活动规则id',
    `activity_rule_id`      STRING COMMENT '参与活动规则id',
    `coupon_id`             STRING COMMENT '使用优惠券id',
    `date_id`               STRING COMMENT '下单日期id',
    `create_time`           STRING COMMENT '下单时间',
    `source_id`             STRING COMMENT '来源编号',
    `source_type_code`      STRING COMMENT '来源类型编码',
    `source_type_name`      STRING COMMENT '来源类型名称',
    `sku_num`               BIGINT COMMENT '商品数量',
    `split_original_amount` DECIMAL(16, 2) COMMENT '原始价格',
    `split_activity_amount` DECIMAL(16, 2) COMMENT '活动优惠分摊',
    `split_coupon_amount`   DECIMAL(16, 2) COMMENT '优惠券优惠分摊',
    `split_total_amount`    DECIMAL(16, 2) COMMENT '最终价格分摊'
) COMMENT '交易域下单明细事务事实表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS ORC
    LOCATION '/warehouse/gmall/dwd/dwd_trade_order_detail_inc/'
    TBLPROPERTIES ('orc.compress' = 'snappy');

-- 数据装载，首日装载
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd_trade_order_detail_inc partition (dt)
select od.id,
       order_id,
       user_id,
       sku_id,
       province_id,
       activity_id,
       activity_rule_id,
       coupon_id,
--     date_format() : 以不同的格式显示日期/时间数据
       date_format(create_time, 'yyyy-MM-dd') date_id,
       create_time,
       source_id,
       source_type,
       dic_name,
       sku_num,
       split_original_amount,
       split_activity_amount,
       split_coupon_amount,
       split_total_amount,
       date_format(create_time, 'yyyy-MM-dd')
from (
         select data.id,
                data.order_id,
                data.sku_id,
                data.create_time,
                data.source_id,
                data.source_type,
                data.sku_num,
                data.sku_num * data.order_price split_original_amount,
                data.split_total_amount,
                data.split_activity_amount,
                data.split_coupon_amount
         from ods_order_detail_inc
         where dt = '2020-06-14'
           and type = 'bootstrap-insert'
     ) od
         left join
     (
         select data.id,
                data.user_id,
                data.province_id
         from ods_order_info_inc
         where dt = '2020-06-14'
           and type = 'bootstrap-insert'
     ) oi
     on od.order_id = oi.id
         left join
     (
         select data.order_detail_id,
                data.activity_id,
                data.activity_rule_id
         from ods_order_detail_activity_inc
         where dt = '2020-06-14'
           and type = 'bootstrap-insert'
     ) act
     on od.id = act.order_detail_id
         left join
     (
         select data.order_detail_id,
                data.coupon_id
         from ods_order_detail_coupon_inc
         where dt = '2020-06-14'
           and type = 'bootstrap-insert'
     ) cou
     on od.id = cou.order_detail_id
         left join
     (
         select dic_code,
                dic_name
         from ods_base_dic_full
         where dt = '2020-06-14'
           and parent_code = '24'
     ) dic
     on od.source_type = dic.dic_code;

-- 每日装载
insert overwrite table dwd_trade_order_detail_inc partition (dt = '2020-06-15')
select od.id,
       order_id,
       user_id,
       sku_id,
       province_id,
       activity_id,
       activity_rule_id,
       coupon_id,
       date_id,
       create_time,
       source_id,
       source_type,
       dic_name,
       sku_num,
       split_original_amount,
       split_activity_amount,
       split_coupon_amount,
       split_total_amount
from (
         select data.id,
                data.order_id,
                data.sku_id,
                date_format(data.create_time, 'yyyy-MM-dd') date_id,
                data.create_time,
                data.source_id,
                data.source_type,
                data.sku_num,
                data.sku_num * data.order_price             split_original_amount,
                data.split_total_amount,
                data.split_activity_amount,
                data.split_coupon_amount
         from ods_order_detail_inc
         where dt = '2020-06-15'
           and type = 'insert'
     ) od
         left join
     (
         select data.id,
                data.user_id,
                data.province_id
         from ods_order_info_inc
         where dt = '2020-06-15'
           and type = 'insert'
     ) oi
     on od.order_id = oi.id
         left join
     (
         select data.order_detail_id,
                data.activity_id,
                data.activity_rule_id
         from ods_order_detail_activity_inc
         where dt = '2020-06-15'
           and type = 'insert'
     ) act
     on od.id = act.order_detail_id
         left join
     (
         select data.order_detail_id,
                data.coupon_id
         from ods_order_detail_coupon_inc
         where dt = '2020-06-15'
           and type = 'insert'
     ) cou
     on od.id = cou.order_detail_id
         left join
     (
         select dic_code,
                dic_name
         from ods_base_dic_full
         where dt = '2020-06-15'
           and parent_code = '24'
     ) dic
     on od.source_type = dic.dic_code;

-- 交易域取消订单事务事实表
CREATE EXTERNAL TABLE dwd_trade_cancel_detail_inc
(
    `id`                    STRING COMMENT '编号',
    `order_id`              STRING COMMENT '订单id',
    `user_id`               STRING COMMENT '用户id',
    `sku_id`                STRING COMMENT '商品id',
    `province_id`           STRING COMMENT '省份id',
    `activity_id`           STRING COMMENT '参与活动规则id',
    `activity_rule_id`      STRING COMMENT '参与活动规则id',
    `coupon_id`             STRING COMMENT '使用优惠券id',
    `date_id`               STRING COMMENT '取消订单日期id',
    `cancel_time`           STRING COMMENT '取消订单时间',
    `source_id`             STRING COMMENT '来源编号',
    `source_type_code`      STRING COMMENT '来源类型编码',
    `source_type_name`      STRING COMMENT '来源类型名称',
    `sku_num`               BIGINT COMMENT '商品数量',
    `split_original_amount` DECIMAL(16, 2) COMMENT '原始价格',
    `split_activity_amount` DECIMAL(16, 2) COMMENT '活动优惠分摊',
    `split_coupon_amount`   DECIMAL(16, 2) COMMENT '优惠券优惠分摊',
    `split_total_amount`    DECIMAL(16, 2) COMMENT '最终价格分摊'
) COMMENT '交易域取消订单明细事务事实表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS ORC
    LOCATION '/warehouse/gmall/dwd/dwd_trade_cancel_detail_inc/'
    TBLPROPERTIES ('orc.compress' = 'snappy');

-- 首日装载
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd_trade_cancel_detail_inc partition (dt)
select od.id,
       order_id,
       user_id,
       sku_id,
       province_id,
       activity_id,
       activity_rule_id,
       coupon_id,
       date_format(canel_time, 'yyyy-MM-dd') date_id,
       canel_time,
       source_id,
       source_type,
       dic_name,
       sku_num,
       split_original_amount,
       split_activity_amount,
       split_coupon_amount,
       split_total_amount,
       date_format(canel_time, 'yyyy-MM-dd')
from (
         select data.id,
                data.order_id,
                data.sku_id,
                data.source_id,
                data.source_type,
                data.sku_num,
                data.sku_num * data.order_price split_original_amount,
                data.split_total_amount,
                data.split_activity_amount,
                data.split_coupon_amount
         from ods_order_detail_inc
         where dt = '2020-06-14'
           and type = 'bootstrap-insert'
     ) od
         join
     (
         select data.id,
                data.user_id,
                data.province_id,
                data.operate_time canel_time
         from ods_order_info_inc
         where dt = '2020-06-14'
           and type = 'bootstrap-insert'
           and data.order_status = '1003'
     ) oi
     on od.order_id = oi.id
         left join
     (
         select data.order_detail_id,
                data.activity_id,
                data.activity_rule_id
         from ods_order_detail_activity_inc
         where dt = '2020-06-14'
           and type = 'bootstrap-insert'
     ) act
     on od.id = act.order_detail_id
         left join
     (
         select data.order_detail_id,
                data.coupon_id
         from ods_order_detail_coupon_inc
         where dt = '2020-06-14'
           and type = 'bootstrap-insert'
     ) cou
     on od.id = cou.order_detail_id
         left join
     (
         select dic_code,
                dic_name
         from ods_base_dic_full
         where dt = '2020-06-14'
           and parent_code = '24'
     ) dic
     on od.source_type = dic.dic_code;

-- 每日装载
insert overwrite table dwd_trade_cancel_detail_inc partition (dt = '2020-06-15')
select od.id,
       order_id,
       user_id,
       sku_id,
       province_id,
       activity_id,
       activity_rule_id,
       coupon_id,
       date_format(canel_time, 'yyyy-MM-dd') date_id,
       canel_time,
       source_id,
       source_type,
       dic_name,
       sku_num,
       split_original_amount,
       split_activity_amount,
       split_coupon_amount,
       split_total_amount
from (
         select data.id,
                data.order_id,
                data.sku_id,
                data.source_id,
                data.source_type,
                data.sku_num,
                data.sku_num * data.order_price split_original_amount,
                data.split_total_amount,
                data.split_activity_amount,
                data.split_coupon_amount
         from ods_order_detail_inc
         where (dt = '2020-06-15' or dt = date_add('2020-06-15', -1))
           and (type = 'insert' or type = 'bootstrap-insert')
     ) od
         join
     (
         select data.id,
                data.user_id,
                data.province_id,
                data.operate_time canel_time
         from ods_order_info_inc
         where dt = '2020-06-15'
           and type = 'update'
           and data.order_status = '1003'
--  map_keys() : 可得map中所有的key; 返回值类型: array
--  array_contains() : 与SQL中的in关键字操作类似，用于判定包含关系
           and array_contains(map_keys(old), 'order_status')
     ) oi
     on order_id = oi.id
         left join
     (
         select data.order_detail_id,
                data.activity_id,
                data.activity_rule_id
         from ods_order_detail_activity_inc
         where (dt = '2020-06-15' or dt = date_add('2020-06-15', -1))
           and (type = 'insert' or type = 'bootstrap-insert')
     ) act
     on od.id = act.order_detail_id
         left join
     (
         select data.order_detail_id,
                data.coupon_id
         from ods_order_detail_coupon_inc
         where (dt = '2020-06-15' or dt = date_add('2020-06-15', -1))
           and (type = 'insert' or type = 'bootstrap-insert')
     ) cou
     on od.id = cou.order_detail_id
         left join
     (
         select dic_code,
                dic_name
         from ods_base_dic_full
         where dt = '2020-06-15'
           and parent_code = '24'
     ) dic
     on od.source_type = dic.dic_code;

-- 交易域支付成功事务事实表
CREATE EXTERNAL TABLE dwd_trade_pay_detail_suc_inc
(
    `id`                    STRING COMMENT '编号',
    `order_id`              STRING COMMENT '订单id',
    `user_id`               STRING COMMENT '用户id',
    `sku_id`                STRING COMMENT '商品id',
    `province_id`           STRING COMMENT '省份id',
    `activity_id`           STRING COMMENT '参与活动规则id',
    `activity_rule_id`      STRING COMMENT '参与活动规则id',
    `coupon_id`             STRING COMMENT '使用优惠券id',
    `payment_type_code`     STRING COMMENT '支付类型编码',
    `payment_type_name`     STRING COMMENT '支付类型名称',
    `date_id`               STRING COMMENT '支付日期id',
    `callback_time`         STRING COMMENT '支付成功时间',
    `source_id`             STRING COMMENT '来源编号',
    `source_type_code`      STRING COMMENT '来源类型编码',
    `source_type_name`      STRING COMMENT '来源类型名称',
    `sku_num`               BIGINT COMMENT '商品数量',
    `split_original_amount` DECIMAL(16, 2) COMMENT '应支付原始金额',
    `split_activity_amount` DECIMAL(16, 2) COMMENT '支付活动优惠分摊',
    `split_coupon_amount`   DECIMAL(16, 2) COMMENT '支付优惠券优惠分摊',
    `split_payment_amount`  DECIMAL(16, 2) COMMENT '支付金额'
) COMMENT '交易域成功支付事务事实表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS ORC
    LOCATION '/warehouse/gmall/dwd/dwd_trade_pay_detail_suc_inc/'
    TBLPROPERTIES ('orc.compress' = 'snappy');

-- 首日装载
insert overwrite table dwd_trade_pay_detail_suc_inc partition (dt)
select od.id,
       od.order_id,
       user_id,
       sku_id,
       province_id,
       activity_id,
       activity_rule_id,
       coupon_id,
       payment_type,
       pay_dic.dic_name,
       date_format(callback_time, 'yyyy-MM-dd') date_id,
       callback_time,
       source_id,
       source_type,
       src_dic.dic_name,
       sku_num,
       split_original_amount,
       split_activity_amount,
       split_coupon_amount,
       split_total_amount,
       date_format(callback_time, 'yyyy-MM-dd')
from (
         select data.id,
                data.order_id,
                data.sku_id,
                data.source_id,
                data.source_type,
                data.sku_num,
                data.sku_num * data.order_price split_original_amount,
                data.split_total_amount,
                data.split_activity_amount,
                data.split_coupon_amount
         from ods_order_detail_inc
         where dt = '2020-06-14'
           and type = 'bootstrap-insert'
     ) od
         join
     (
         select data.user_id,
                data.order_id,
                data.payment_type,
                data.callback_time
         from ods_payment_info_inc
         where dt = '2020-06-14'
           and type = 'bootstrap-insert'
           and data.payment_status = '1602'
     ) pi
     on od.order_id = pi.order_id
         left join
     (
         select data.id,
                data.province_id
         from ods_order_info_inc
         where dt = '2020-06-14'
           and type = 'bootstrap-insert'
     ) oi
     on od.order_id = oi.id
         left join
     (
         select data.order_detail_id,
                data.activity_id,
                data.activity_rule_id
         from ods_order_detail_activity_inc
         where dt = '2020-06-14'
           and type = 'bootstrap-insert'
     ) act
     on od.id = act.order_detail_id
         left join
     (
         select data.order_detail_id,
                data.coupon_id
         from ods_order_detail_coupon_inc
         where dt = '2020-06-14'
           and type = 'bootstrap-insert'
     ) cou
     on od.id = cou.order_detail_id
         left join
     (
         select dic_code,
                dic_name
         from ods_base_dic_full
         where dt = '2020-06-14'
           and parent_code = '11'
     ) pay_dic
     on pi.payment_type = pay_dic.dic_code
         left join
     (
         select dic_code,
                dic_name
         from ods_base_dic_full
         where dt = '2020-06-14'
           and parent_code = '24'
     ) src_dic
     on od.source_type = src_dic.dic_code;

-- 每日装载
insert overwrite table dwd_trade_pay_detail_suc_inc partition (dt = '2020-06-15')
select od.id,
       od.order_id,
       user_id,
       sku_id,
       province_id,
       activity_id,
       activity_rule_id,
       coupon_id,
       payment_type,
       pay_dic.dic_name,
       date_format(callback_time, 'yyyy-MM-dd') date_id,
       callback_time,
       source_id,
       source_type,
       src_dic.dic_name,
       sku_num,
       split_original_amount,
       split_activity_amount,
       split_coupon_amount,
       split_total_amount
from (
         select data.id,
                data.order_id,
                data.sku_id,
                data.source_id,
                data.source_type,
                data.sku_num,
                data.sku_num * data.order_price split_original_amount,
                data.split_total_amount,
                data.split_activity_amount,
                data.split_coupon_amount
         from ods_order_detail_inc
         where (dt = '2020-06-15' or dt = date_add('2020-06-15', -1))
           and (type = 'insert' or type = 'bootstrap-insert')
     ) od
         join
     (
         select data.user_id,
                data.order_id,
                data.payment_type,
                data.callback_time
         from ods_payment_info_inc
         where dt = '2020-06-15'
           and type = 'update'
           and array_contains(map_keys(old), 'payment_status')
           and data.payment_status = '1602'
     ) pi
     on od.order_id = pi.order_id
         left join
     (
         select data.id,
                data.province_id
         from ods_order_info_inc
         where (dt = '2020-06-15' or dt = date_add('2020-06-15', -1))
           and (type = 'insert' or type = 'bootstrap-insert')
     ) oi
     on od.order_id = oi.id
         left join
     (
         select data.order_detail_id,
                data.activity_id,
                data.activity_rule_id
         from ods_order_detail_activity_inc
         where (dt = '2020-06-15' or dt = date_add('2020-06-15', -1))
           and (type = 'insert' or type = 'bootstrap-insert')
     ) act
     on od.id = act.order_detail_id
         left join
     (
         select data.order_detail_id,
                data.coupon_id
         from ods_order_detail_coupon_inc
         where (dt = '2020-06-15' or dt = date_add('2020-06-15', -1))
           and (type = 'insert' or type = 'bootstrap-insert')
     ) cou
     on od.id = cou.order_detail_id
         left join
     (
         select dic_code,
                dic_name
         from ods_base_dic_full
         where dt = '2020-06-15'
           and parent_code = '11'
     ) pay_dic
     on pi.payment_type = pay_dic.dic_code
         left join
     (
         select dic_code,
                dic_name
         from ods_base_dic_full
         where dt = '2020-06-15'
           and parent_code = '24'
     ) src_dic
     on od.source_type = src_dic.dic_code;

-- 交易域退单事务事实表
CREATE EXTERNAL TABLE dwd_trade_order_refund_inc
(
    `id`                      STRING COMMENT '编号',
    `user_id`                 STRING COMMENT '用户ID',
    `order_id`                STRING COMMENT '订单ID',
    `sku_id`                  STRING COMMENT '商品ID',
    `province_id`             STRING COMMENT '地区ID',
    `date_id`                 STRING COMMENT '日期ID',
    `create_time`             STRING COMMENT '退单时间',
    `refund_type_code`        STRING COMMENT '退单类型编码',
    `refund_type_name`        STRING COMMENT '退单类型名称',
    `refund_reason_type_code` STRING COMMENT '退单原因类型编码',
    `refund_reason_type_name` STRING COMMENT '退单原因类型名称',
    `refund_reason_txt`       STRING COMMENT '退单原因描述',
    `refund_num`              BIGINT COMMENT '退单件数',
    `refund_amount`           DECIMAL(16, 2) COMMENT '退单金额'
) COMMENT '交易域退单事务事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dwd/dwd_trade_order_refund_inc/'
    TBLPROPERTIES ("orc.compress" = "snappy");

-- 数据装载，首日装载
insert overwrite table dwd_trade_order_refund_inc partition (dt)
select ri.id,
       user_id,
       order_id,
       sku_id,
       province_id,
       date_format(create_time, 'yyyy-MM-dd') date_id,
       create_time,
       refund_type,
       type_dic.dic_name,
       refund_reason_type,
       reason_dic.dic_name,
       refund_reason_txt,
       refund_num,
       refund_amount,
       date_format(create_time, 'yyyy-MM-dd')
from (
         select data.id,
                data.user_id,
                data.order_id,
                data.sku_id,
                data.refund_type,
                data.refund_num,
                data.refund_amount,
                data.refund_reason_type,
                data.refund_reason_txt,
                data.create_time
         from ods_order_refund_info_inc
         where dt = '2020-06-14'
           and type = 'bootstrap-insert'
     ) ri
         left join
     (
         select data.id,
                data.province_id
         from ods_order_info_inc
         where dt = '2020-06-14'
           and type = 'bootstrap-insert'
     ) oi
     on ri.order_id = oi.id
         left join
     (
         select dic_code,
                dic_name
         from ods_base_dic_full
         where dt = '2020-06-14'
           and parent_code = '15'
     ) type_dic
     on ri.refund_type = type_dic.dic_code
         left join
     (
         select dic_code,
                dic_name
         from ods_base_dic_full
         where dt = '2020-06-14'
           and parent_code = '13'
     ) reason_dic
     on ri.refund_reason_type = reason_dic.dic_code;

-- 每日装载
insert overwrite table dwd_trade_order_refund_inc partition (dt = '2020-06-15')
select ri.id,
       user_id,
       order_id,
       sku_id,
       province_id,
       date_format(create_time, 'yyyy-MM-dd') date_id,
       create_time,
       refund_type,
       type_dic.dic_name,
       refund_reason_type,
       reason_dic.dic_name,
       refund_reason_txt,
       refund_num,
       refund_amount
from (
         select data.id,
                data.user_id,
                data.order_id,
                data.sku_id,
                data.refund_type,
                data.refund_num,
                data.refund_amount,
                data.refund_reason_type,
                data.refund_reason_txt,
                data.create_time
         from ods_order_refund_info_inc
         where dt = '2020-06-15'
           and type = 'insert'
     ) ri
         left join
     (
         select data.id,
                data.province_id
         from ods_order_info_inc
         where dt = '2020-06-15'
           and type = 'update'
           and data.order_status = '1005'
           and array_contains(map_keys(old), 'order_status')
     ) oi
     on ri.order_id = oi.id
         left join
     (
         select dic_code,
                dic_name
         from ods_base_dic_full
         where dt = '2020-06-15'
           and parent_code = '15'
     ) type_dic
     on ri.refund_type = type_dic.dic_code
         left join
     (
         select dic_code,
                dic_name
         from ods_base_dic_full
         where dt = '2020-06-15'
           and parent_code = '13'
     ) reason_dic
     on ri.refund_reason_type = reason_dic.dic_code;

-- 交易域退款成功事务事实表
CREATE EXTERNAL TABLE dwd_trade_refund_pay_suc_inc
(
    `id`                STRING COMMENT '编号',
    `user_id`           STRING COMMENT '用户ID',
    `order_id`          STRING COMMENT '订单编号',
    `sku_id`            STRING COMMENT 'SKU编号',
    `province_id`       STRING COMMENT '地区ID',
    `payment_type_code` STRING COMMENT '支付类型编码',
    `payment_type_name` STRING COMMENT '支付类型名称',
    `date_id`           STRING COMMENT '日期ID',
    `callback_time`     STRING COMMENT '支付成功时间',
    `refund_num`        DECIMAL(16, 2) COMMENT '退款件数',
    `refund_amount`     DECIMAL(16, 2) COMMENT '退款金额'
) COMMENT '交易域提交退款成功事务事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dwd/dwd_trade_refund_pay_suc_inc/'
    TBLPROPERTIES ("orc.compress" = "snappy");

-- 数据装载，首日装载
insert overwrite table dwd_trade_refund_pay_suc_inc partition (dt)
select rp.id,
       user_id,
       rp.order_id,
       rp.sku_id,
       province_id,
       payment_type,
       dic_name,
       date_format(callback_time, 'yyyy-MM-dd') date_id,
       callback_time,
       refund_num,
       total_amount,
       date_format(callback_time, 'yyyy-MM-dd')
from (
         select data.id,
                data.order_id,
                data.sku_id,
                data.payment_type,
                data.callback_time,
                data.total_amount
         from ods_refund_payment_inc
         where dt = '2020-06-14'
           and type = 'bootstrap-insert'
           and data.refund_status = '1602'
     ) rp
         left join
     (
         select data.id,
                data.user_id,
                data.province_id
         from ods_order_info_inc
         where dt = '2020-06-14'
           and type = 'bootstrap-insert'
     ) oi
     on rp.order_id = oi.id
         left join
     (
         select data.order_id,
                data.sku_id,
                data.refund_num
         from ods_order_refund_info_inc
         where dt = '2020-06-14'
           and type = 'bootstrap-insert'
     ) ri
     on rp.order_id = ri.order_id
         and rp.sku_id = ri.sku_id
         left join
     (
         select dic_code,
                dic_name
         from ods_base_dic_full
         where dt = '2020-06-14'
           and parent_code = '11'
     ) dic
     on rp.payment_type = dic.dic_code;

-- 每日装载
insert overwrite table dwd_trade_refund_pay_suc_inc partition (dt = '2020-06-15')
select rp.id,
       user_id,
       rp.order_id,
       rp.sku_id,
       province_id,
       payment_type,
       dic_name,
       date_format(callback_time, 'yyyy-MM-dd') date_id,
       callback_time,
       refund_num,
       total_amount
from (
         select data.id,
                data.order_id,
                data.sku_id,
                data.payment_type,
                data.callback_time,
                data.total_amount
         from ods_refund_payment_inc
         where dt = '2020-06-15'
           and type = 'update'
           and array_contains(map_keys(old), 'refund_status')
           and data.refund_status = '1602'
     ) rp
         left join
     (
         select data.id,
                data.user_id,
                data.province_id
         from ods_order_info_inc
         where dt = '2020-06-15'
           and type = 'update'
           and data.order_status = '1006'
           and array_contains(map_keys(old), 'order_status')
     ) oi
     on rp.order_id = oi.id
         left join
     (
         select data.order_id,
                data.sku_id,
                data.refund_num
         from ods_order_refund_info_inc
         where dt = '2020-06-15'
           and type = 'update'
           and data.refund_status = '0705'
           and array_contains(map_keys(old), 'refund_status')
     ) ri
     on rp.order_id = ri.order_id
         and rp.sku_id = ri.sku_id
         left join
     (
         select dic_code,
                dic_name
         from ods_base_dic_full
         where dt = '2020-06-15'
           and parent_code = '11'
     ) dic
     on rp.payment_type = dic.dic_code;

-- 交易域购物车周期快照事实表
CREATE EXTERNAL TABLE dwd_trade_cart_full
(
    `id`       STRING COMMENT '编号',
    `user_id`  STRING COMMENT '用户id',
    `sku_id`   STRING COMMENT '商品id',
    `sku_name` STRING COMMENT '商品名称',
    `sku_num`  BIGINT COMMENT '加购物车件数'
) COMMENT '交易域购物车周期快照事实表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS ORC
    LOCATION '/warehouse/gmall/dwd/dwd_trade_cart_full/'
    TBLPROPERTIES ('orc.compress' = 'snappy');

-- 数据装载
insert overwrite table dwd_trade_cart_full partition (dt = '2020-06-14')
select id,
       user_id,
       sku_id,
       sku_name,
       sku_num
from ods_cart_info_full
where dt = '2020-06-14'
  and is_ordered = '0';

-- 工具域优惠券领取事务事实表
CREATE EXTERNAL TABLE dwd_tool_coupon_get_inc
(
    `id`        STRING COMMENT '编号',
    `coupon_id` STRING COMMENT '优惠券ID',
    `user_id`   STRING COMMENT 'userid',
    `date_id`   STRING COMMENT '日期ID',
    `get_time`  STRING COMMENT '领取时间'
) COMMENT '优惠券领取事务事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dwd/dwd_tool_coupon_get_inc/'
    TBLPROPERTIES ("orc.compress" = "snappy");

-- 数据装载，首日装载
insert overwrite table dwd_tool_coupon_get_inc partition (dt)
select data.id,
       data.coupon_id,
       data.user_id,
       date_format(data.get_time, 'yyyy-MM-dd') date_id,
       data.get_time,
       date_format(data.get_time, 'yyyy-MM-dd')
from ods_coupon_use_inc
where dt = '2020-06-14'
  and type = 'bootstrap-insert';

-- 每日装载
insert overwrite table dwd_tool_coupon_get_inc partition (dt = '2020-06-15')
select data.id,
       data.coupon_id,
       data.user_id,
       date_format(data.get_time, 'yyyy-MM-dd') date_id,
       data.get_time
from ods_coupon_use_inc
where dt = '2020-06-15'
  and type = 'insert';

-- 工具域优惠券使用(下单)事务事实表
CREATE EXTERNAL TABLE dwd_tool_coupon_order_inc
(
    `id`         STRING COMMENT '编号',
    `coupon_id`  STRING COMMENT '优惠券ID',
    `user_id`    STRING COMMENT 'user_id',
    `order_id`   STRING COMMENT 'order_id',
    `date_id`    STRING COMMENT '日期ID',
    `order_time` STRING COMMENT '使用下单时间'
) COMMENT '优惠券使用下单事务事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dwd/dwd_tool_coupon_order_inc/'
    TBLPROPERTIES ("orc.compress" = "snappy");

-- 数据装载，首日装载
insert overwrite table dwd_tool_coupon_order_inc partition (dt)
select data.id,
       data.coupon_id,
       data.user_id,
       data.order_id,
       date_format(data.using_time, 'yyyy-MM-dd') date_id,
       data.using_time,
       date_format(data.using_time, 'yyyy-MM-dd')
from ods_coupon_use_inc
where dt = '2020-06-14'
  and type = 'bootstrap-insert'
  and data.using_time is not null;

-- 每日装载
insert overwrite table dwd_tool_coupon_order_inc partition (dt = '2020-06-15')
select data.id,
       data.coupon_id,
       data.user_id,
       data.order_id,
       date_format(data.using_time, 'yyyy-MM-dd') date_id,
       data.using_time
from ods_coupon_use_inc
where dt = '2020-06-15'
  and type = 'update'
  and array_contains(map_keys(old), 'using_time');

-- 工具域优惠券使用(支付)事务事实表
CREATE EXTERNAL TABLE dwd_tool_coupon_pay_inc
(
    `id`           STRING COMMENT '编号',
    `coupon_id`    STRING COMMENT '优惠券ID',
    `user_id`      STRING COMMENT 'user_id',
    `order_id`     STRING COMMENT 'order_id',
    `date_id`      STRING COMMENT '日期ID',
    `payment_time` STRING COMMENT '使用下单时间'
) COMMENT '优惠券使用支付事务事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dwd/dwd_tool_coupon_pay_inc/'
    TBLPROPERTIES ("orc.compress" = "snappy");

-- 数据装载，首日装载
insert overwrite table dwd_tool_coupon_pay_inc partition (dt)
select data.id,
       data.coupon_id,
       data.user_id,
       data.order_id,
       date_format(data.used_time, 'yyyy-MM-dd') date_id,
       data.used_time,
       date_format(data.used_time, 'yyyy-MM-dd')
from ods_coupon_use_inc
where dt = '2020-06-14'
  and type = 'bootstrap-insert'
  and data.used_time is not null;

-- 每日装载
insert overwrite table dwd_tool_coupon_pay_inc partition (dt = '2020-06-15')
select data.id,
       data.coupon_id,
       data.user_id,
       data.order_id,
       date_format(data.used_time, 'yyyy-MM-dd') date_id,
       data.used_time
from ods_coupon_use_inc
where dt = '2020-06-15'
  and type = 'update'
  and array_contains(map_keys(old), 'used_time');

-- 互动域收藏商品事务事实表
CREATE EXTERNAL TABLE dwd_interaction_favor_add_inc
(
    `id`          STRING COMMENT '编号',
    `user_id`     STRING COMMENT '用户id',
    `sku_id`      STRING COMMENT 'sku_id',
    `date_id`     STRING COMMENT '日期id',
    `create_time` STRING COMMENT '收藏时间'
) COMMENT '收藏事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dwd/dwd_interaction_favor_add_inc/'
    TBLPROPERTIES ("orc.compress" = "snappy");

-- 数据装载，首日装载
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd_interaction_favor_add_inc partition (dt)
select data.id,
       data.user_id,
       data.sku_id,
       date_format(data.create_time, 'yyyy-MM-dd') date_id,
       data.create_time,
       date_format(data.create_time, 'yyyy-MM-dd')
from ods_favor_info_inc
where dt = '2020-06-14'
  and type = 'bootstrap-insert';

-- 每日装载
insert overwrite table dwd_interaction_favor_add_inc partition (dt = '2020-06-15')
select data.id,
       data.user_id,
       data.sku_id,
       date_format(data.create_time, 'yyyy-MM-dd') date_id,
       data.create_time
from ods_favor_info_inc
where dt = '2020-06-15'
  and type = 'insert';

-- 互动域评价事务事实表
CREATE EXTERNAL TABLE dwd_interaction_comment_inc
(
    `id`            STRING COMMENT '编号',
    `user_id`       STRING COMMENT '用户ID',
    `sku_id`        STRING COMMENT 'sku_id',
    `order_id`      STRING COMMENT '订单ID',
    `date_id`       STRING COMMENT '日期ID',
    `create_time`   STRING COMMENT '评价时间',
    `appraise_code` STRING COMMENT '评价编码',
    `appraise_name` STRING COMMENT '评价名称'
) COMMENT '评价事务事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dwd/dwd_interaction_comment_inc/'
    TBLPROPERTIES ("orc.compress" = "snappy");

-- 数据装载，首日装载
insert overwrite table dwd_interaction_comment_inc partition (dt)
select id,
       user_id,
       sku_id,
       order_id,
       date_format(create_time, 'yyyy-MM-dd') date_id,
       create_time,
       appraise,
       dic_name,
       date_format(create_time, 'yyyy-MM-dd')
from (
         select data.id,
                data.user_id,
                data.sku_id,
                data.order_id,
                data.create_time,
                data.appraise
         from ods_comment_info_inc
         where dt = '2020-06-14'
           and type = 'bootstrap-insert'
     ) ci
         left join
     (
         select dic_code,
                dic_name
         from ods_base_dic_full
         where dt = '2020-06-14'
           and parent_code = '12'
     ) dic
     on ci.appraise = dic.dic_code;

-- 每日装载
insert overwrite table dwd_interaction_comment_inc partition (dt = '2020-06-15')
select id,
       user_id,
       sku_id,
       order_id,
       date_format(create_time, 'yyyy-MM-dd') date_id,
       create_time,
       appraise,
       dic_name
from (
         select data.id,
                data.user_id,
                data.sku_id,
                data.order_id,
                data.create_time,
                data.appraise
         from ods_comment_info_inc
         where dt = '2020-06-15'
           and type = 'insert'
     ) ci
         left join
     (
         select dic_code,
                dic_name
         from ods_base_dic_full
         where dt = '2020-06-15'
           and parent_code = '12'
     ) dic
     on ci.appraise = dic.dic_code;

-- 流量域页面浏览事务事实表
CREATE EXTERNAL TABLE dwd_traffic_page_view_inc
(
    `province_id`    STRING COMMENT '省份id',
    `brand`          STRING COMMENT '手机品牌',
    `channel`        STRING COMMENT '渠道',
    `is_new`         STRING COMMENT '是否首次启动',
    `model`          STRING COMMENT '手机型号',
    `mid_id`         STRING COMMENT '设备id',
    `operate_system` STRING COMMENT '操作系统',
    `user_id`        STRING COMMENT '会员id',
    `version_code`   STRING COMMENT 'app版本号',
    `page_item`      STRING COMMENT '目标id ',
    `page_item_type` STRING COMMENT '目标类型',
    `last_page_id`   STRING COMMENT '上页类型',
    `page_id`        STRING COMMENT '页面ID ',
    `source_type`    STRING COMMENT '来源类型',
    `date_id`        STRING COMMENT '日期id',
    `view_time`      STRING COMMENT '跳入时间',
    `session_id`     STRING COMMENT '所属会话id',
    `during_time`    BIGINT COMMENT '持续时间毫秒'
) COMMENT '页面日志表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dwd/dwd_traffic_page_view_inc'
    TBLPROPERTIES ('orc.compress' = 'snappy');

-- 数据装载，关闭hive的 CBO 筛选结构体不为null的数据
set hive.cbo.enable=false;
insert overwrite table dwd_traffic_page_view_inc partition (dt = '2020-06-14')
select province_id,
       brand,
       channel,
       is_new,
       model,
       mid_id,
       operate_system,
       user_id,
       version_code,
       page_item,
       page_item_type,
       last_page_id,
       page_id,
       source_type,
       date_format(from_utc_timestamp(ts, 'GMT+8'), 'yyyy-MM-dd')                                        date_id,
       date_format(from_utc_timestamp(ts, 'GMT+8'), 'yyyy-MM-dd HH:mm:ss')                               view_time,
       concat(mid_id, '-', last_value(session_start_point, true) over (partition by mid_id order by ts)) session_id,
       during_time
from (
         select common.ar                               area_code,
                common.ba                               brand,
                common.ch                               channel,
                common.is_new                           is_new,
                common.md                               model,
                common.mid                              mid_id,
                common.os                               operate_system,
                common.uid                              user_id,
                common.vc                               version_code,
                page.during_time,
                page.item                               page_item,
                page.item_type                          page_item_type,
                page.last_page_id,
                page.page_id,
                page.source_type,
                ts,
                if(page.last_page_id is null, ts, null) session_start_point
         from ods_log_inc
         where dt = '2020-06-14'
           and page is not null
     ) log
         left join
     (
         select id province_id,
                area_code
         from ods_base_province_full
         where dt = '2020-06-14'
     ) bp
     on log.area_code = bp.area_code;

-- 流量域启动事务事实表
CREATE EXTERNAL TABLE dwd_traffic_start_inc
(
    `province_id`     STRING COMMENT '省份id',
    `brand`           STRING COMMENT '手机品牌',
    `channel`         STRING COMMENT '渠道',
    `is_new`          STRING COMMENT '是否首次启动',
    `model`           STRING COMMENT '手机型号',
    `mid_id`          STRING COMMENT '设备id',
    `operate_system`  STRING COMMENT '操作系统',
    `user_id`         STRING COMMENT '会员id',
    `version_code`    STRING COMMENT 'app版本号',
    `entry`           STRING COMMENT 'icon手机图标 notice 通知',
    `open_ad_id`      STRING COMMENT '广告页ID ',
    `date_id`         STRING COMMENT '日期id',
    `start_time`      STRING COMMENT '启动时间',
    `loading_time_ms` BIGINT COMMENT '启动加载时间',
    `open_ad_ms`      BIGINT COMMENT '广告总共播放时间',
    `open_ad_skip_ms` BIGINT COMMENT '用户跳过广告时点'
) COMMENT '启动日志表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dwd/dwd_traffic_start_inc'
    TBLPROPERTIES ('orc.compress' = 'snappy');

-- 数据装载
set hive.cbo.enable=false;
insert overwrite table dwd_traffic_start_inc partition (dt = '2020-06-14')
select province_id,
       brand,
       channel,
       is_new,
       model,
       mid_id,
       operate_system,
       user_id,
       version_code,
       entry,
       open_ad_id,
       date_format(from_utc_timestamp(ts, 'GMT+8'), 'yyyy-MM-dd')          date_id,
       date_format(from_utc_timestamp(ts, 'GMT+8'), 'yyyy-MM-dd HH:mm:ss') action_time,
       loading_time,
       open_ad_ms,
       open_ad_skip_ms
from (
         select common.ar  area_code,
                common.ba  brand,
                common.ch  channel,
                common.is_new,
                common.md  model,
                common.mid mid_id,
                common.os  operate_system,
                common.uid user_id,
                common.vc  version_code,
                `start`.entry,
                `start`.loading_time,
                `start`.open_ad_id,
                `start`.open_ad_ms,
                `start`.open_ad_skip_ms,
                ts
         from ods_log_inc
         where dt = '2020-06-14'
           and `start` is not null
     ) log
         left join
     (
         select id province_id,
                area_code
         from ods_base_province_full
         where dt = '2020-06-14'
     ) bp
     on log.area_code = bp.area_code;

-- 流量域动作事务事实表
CREATE EXTERNAL TABLE dwd_traffic_action_inc
(
    `province_id`      STRING COMMENT '省份id',
    `brand`            STRING COMMENT '手机品牌',
    `channel`          STRING COMMENT '渠道',
    `is_new`           STRING COMMENT '是否首次启动',
    `model`            STRING COMMENT '手机型号',
    `mid_id`           STRING COMMENT '设备id',
    `operate_system`   STRING COMMENT '操作系统',
    `user_id`          STRING COMMENT '会员id',
    `version_code`     STRING COMMENT 'app版本号',
    `during_time`      BIGINT COMMENT '持续时间毫秒',
    `page_item`        STRING COMMENT '目标id ',
    `page_item_type`   STRING COMMENT '目标类型',
    `last_page_id`     STRING COMMENT '上页类型',
    `page_id`          STRING COMMENT '页面id ',
    `source_type`      STRING COMMENT '来源类型',
    `action_id`        STRING COMMENT '动作id',
    `action_item`      STRING COMMENT '目标id ',
    `action_item_type` STRING COMMENT '目标类型',
    `date_id`          STRING COMMENT '日期id',
    `action_time`      STRING COMMENT '动作发生时间'
) COMMENT '动作日志表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dwd/dwd_traffic_action_inc'
    TBLPROPERTIES ('orc.compress' = 'snappy');

-- 数据装载
set hive.cbo.enable=false;
insert overwrite table dwd_traffic_action_inc partition (dt = '2020-06-14')
select province_id,
       brand,
       channel,
       is_new,
       model,
       mid_id,
       operate_system,
       user_id,
       version_code,
       during_time,
       page_item,
       page_item_type,
       last_page_id,
       page_id,
       source_type,
       action_id,
       action_item,
       action_item_type,
       date_format(from_utc_timestamp(ts, 'GMT+8'), 'yyyy-MM-dd')          date_id,
       date_format(from_utc_timestamp(ts, 'GMT+8'), 'yyyy-MM-dd HH:mm:ss') action_time
from (
         select common.ar        area_code,
                common.ba        brand,
                common.ch        channel,
                common.is_new,
                common.md        model,
                common.mid       mid_id,
                common.os        operate_system,
                common.uid       user_id,
                common.vc        version_code,
                page.during_time,
                page.item        page_item,
                page.item_type   page_item_type,
                page.last_page_id,
                page.page_id,
                page.source_type,
                action.action_id,
                action.item      action_item,
                action.item_type action_item_type,
                action.ts
--       explode() : 列转行（一列转多行），一进多出
--       lateral view : 用于和 split, explode 等 UDTF 一起使用，将炸开的列进行横向填充
         from ods_log_inc lateral view explode(actions) tmp as action
         where dt = '2020-06-14'
           and actions is not null
     ) log
         left join
     (
         select id province_id,
                area_code
         from ods_base_province_full
         where dt = '2020-06-14'
     ) bp
     on log.area_code = bp.area_code;

-- 流量域曝光事务事实表
CREATE EXTERNAL TABLE dwd_traffic_display_inc
(
    `province_id`       STRING COMMENT '省份id',
    `brand`             STRING COMMENT '手机品牌',
    `channel`           STRING COMMENT '渠道',
    `is_new`            STRING COMMENT '是否首次启动',
    `model`             STRING COMMENT '手机型号',
    `mid_id`            STRING COMMENT '设备id',
    `operate_system`    STRING COMMENT '操作系统',
    `user_id`           STRING COMMENT '会员id',
    `version_code`      STRING COMMENT 'app版本号',
    `during_time`       BIGINT COMMENT 'app版本号',
    `page_item`         STRING COMMENT '目标id ',
    `page_item_type`    STRING COMMENT '目标类型',
    `last_page_id`      STRING COMMENT '上页类型',
    `page_id`           STRING COMMENT '页面ID ',
    `source_type`       STRING COMMENT '来源类型',
    `date_id`           STRING COMMENT '日期id',
    `display_time`      STRING COMMENT '曝光时间',
    `display_type`      STRING COMMENT '曝光类型',
    `display_item`      STRING COMMENT '曝光对象id ',
    `display_item_type` STRING COMMENT 'app版本号',
    `display_order`     BIGINT COMMENT '曝光顺序',
    `display_pos_id`    BIGINT COMMENT '曝光位置'
) COMMENT '曝光日志表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dwd/dwd_traffic_display_inc'
    TBLPROPERTIES ('orc.compress' = 'snappy');

-- 数据装载
set hive.cbo.enable=false;
insert overwrite table dwd_traffic_display_inc partition (dt = '2020-06-14')
select province_id,
       brand,
       channel,
       is_new,
       model,
       mid_id,
       operate_system,
       user_id,
       version_code,
       during_time,
       page_item,
       page_item_type,
       last_page_id,
       page_id,
       source_type,
       date_format(from_utc_timestamp(ts, 'GMT+8'), 'yyyy-MM-dd')          date_id,
       date_format(from_utc_timestamp(ts, 'GMT+8'), 'yyyy-MM-dd HH:mm:ss') display_time,
       display_type,
       display_item,
       display_item_type,
       display_order,
       display_pos_id
from (
         select common.ar         area_code,
                common.ba         brand,
                common.ch         channel,
                common.is_new,
                common.md         model,
                common.mid        mid_id,
                common.os         operate_system,
                common.uid        user_id,
                common.vc         version_code,
                page.during_time,
                page.item         page_item,
                page.item_type    page_item_type,
                page.last_page_id,
                page.page_id,
                page.source_type,
                display.display_type,
                display.item      display_item,
                display.item_type display_item_type,
                display.`order`   display_order,
                display.pos_id    display_pos_id,
                ts
         from ods_log_inc lateral view explode(displays) tmp as display
         where dt = '2020-06-14'
           and displays is not null
     ) log
         left join
     (
         select id province_id,
                area_code
         from ods_base_province_full
         where dt = '2020-06-14'
     ) bp
     on log.area_code = bp.area_code;

-- 流量域错误事务事实表
CREATE EXTERNAL TABLE dwd_traffic_error_inc
(
    `province_id`     STRING COMMENT '地区编码',
    `brand`           STRING COMMENT '手机品牌',
    `channel`         STRING COMMENT '渠道',
    `is_new`          STRING COMMENT '是否首次启动',
    `model`           STRING COMMENT '手机型号',
    `mid_id`          STRING COMMENT '设备id',
    `operate_system`  STRING COMMENT '操作系统',
    `user_id`         STRING COMMENT '会员id',
    `version_code`    STRING COMMENT 'app版本号',
    `page_item`       STRING COMMENT '目标id ',
    `page_item_type`  STRING COMMENT '目标类型',
    `last_page_id`    STRING COMMENT '上页类型',
    `page_id`         STRING COMMENT '页面ID ',
    `source_type`     STRING COMMENT '来源类型',
    `entry`           STRING COMMENT 'icon手机图标  notice 通知',
    `loading_time`    STRING COMMENT '启动加载时间',
    `open_ad_id`      STRING COMMENT '广告页ID ',
    `open_ad_ms`      STRING COMMENT '广告总共播放时间',
    `open_ad_skip_ms` STRING COMMENT '用户跳过广告时点',
    `actions`         ARRAY<STRUCT<action_id:STRING,item:STRING,item_type:STRING,ts:BIGINT>> COMMENT '动作信息',
    `displays`        ARRAY<STRUCT<display_type :STRING,item :STRING,item_type :STRING,`order` :STRING,pos_id
                                   :STRING>> COMMENT '曝光信息',
    `date_id`         STRING COMMENT '日期id',
    `error_time`      STRING COMMENT '错误时间',
    `error_code`      STRING COMMENT '错误码',
    `error_msg`       STRING COMMENT '错误信息'
) COMMENT '错误日志表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dwd/dwd_traffic_error_inc'
    TBLPROPERTIES ('orc.compress' = 'snappy');

-- 数据装载
set hive.cbo.enable=false;
-- 切换hive的引擎
set hive.execution.engine=mr;
insert overwrite table dwd_traffic_error_inc partition (dt = '2020-06-14')
select province_id,
       brand,
       channel,
       is_new,
       model,
       mid_id,
       operate_system,
       user_id,
       version_code,
       page_item,
       page_item_type,
       last_page_id,
       page_id,
       source_type,
       entry,
       loading_time,
       open_ad_id,
       open_ad_ms,
       open_ad_skip_ms,
       actions,
       displays,
       date_format(from_utc_timestamp(ts, 'GMT+8'), 'yyyy-MM-dd')          date_id,
       date_format(from_utc_timestamp(ts, 'GMT+8'), 'yyyy-MM-dd HH:mm:ss') error_time,
       error_code,
       error_msg
from (
         select common.ar      area_code,
                common.ba      brand,
                common.ch      channel,
                common.is_new,
                common.md      model,
                common.mid     mid_id,
                common.os      operate_system,
                common.uid     user_id,
                common.vc      version_code,
                page.during_time,
                page.item      page_item,
                page.item_type page_item_type,
                page.last_page_id,
                page.page_id,
                page.source_type,
                `start`.entry,
                `start`.loading_time,
                `start`.open_ad_id,
                `start`.open_ad_ms,
                `start`.open_ad_skip_ms,
                actions,
                displays,
                err.error_code,
                err.msg        error_msg,
                ts
         from ods_log_inc
         where dt = '2020-06-14'
           and err is not null
     ) log
         join
     (
         select id province_id,
                area_code
         from ods_base_province_full
         where dt = '2020-06-14'
     ) bp
     on log.area_code = bp.area_code;
set hive.execution.engine=spark;

-- 用户域用户注册事务事实表
CREATE EXTERNAL TABLE dwd_user_register_inc
(
    `user_id`        STRING COMMENT '用户ID',
    `date_id`        STRING COMMENT '日期ID',
    `create_time`    STRING COMMENT '注册时间',
    `channel`        STRING COMMENT '应用下载渠道',
    `province_id`    STRING COMMENT '省份id',
    `version_code`   STRING COMMENT '应用版本',
    `mid_id`         STRING COMMENT '设备id',
    `brand`          STRING COMMENT '设备品牌',
    `model`          STRING COMMENT '设备型号',
    `operate_system` STRING COMMENT '设备操作系统'
) COMMENT '用户域用户注册事务事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dwd/dwd_user_register_inc/'
    TBLPROPERTIES ("orc.compress" = "snappy");

-- 数据装载，首日装载
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd_user_register_inc partition (dt)
select ui.user_id,
       date_format(create_time, 'yyyy-MM-dd') date_id,
       create_time,
       channel,
       province_id,
       version_code,
       mid_id,
       brand,
       model,
       operate_system,
       date_format(create_time, 'yyyy-MM-dd')
from (
         select data.id user_id,
                data.create_time
         from ods_user_info_inc
         where dt = '2020-06-14'
           and type = 'bootstrap-insert'
     ) ui
         left join
     (
         select common.ar  area_code,
                common.ba  brand,
                common.ch  channel,
                common.md  model,
                common.mid mid_id,
                common.os  operate_system,
                common.uid user_id,
                common.vc  version_code
         from ods_log_inc
         where dt = '2020-06-14'
           and page.page_id = 'register'
           and common.uid is not null
     ) log
     on ui.user_id = log.user_id
         left join
     (
         select id province_id,
                area_code
         from ods_base_province_full
         where dt = '2020-06-14'
     ) bp
     on log.area_code = bp.area_code;

-- 每日装载
insert overwrite table dwd_user_register_inc partition (dt = '2020-06-15')
select ui.user_id,
       date_format(create_time, 'yyyy-MM-dd') date_id,
       create_time,
       channel,
       province_id,
       version_code,
       mid_id,
       brand,
       model,
       operate_system
from (
         select data.id user_id,
                data.create_time
         from ods_user_info_inc
         where dt = '2020-06-15'
           and type = 'insert'
     ) ui
         left join
     (
         select common.ar  area_code,
                common.ba  brand,
                common.ch  channel,
                common.md  model,
                common.mid mid_id,
                common.os  operate_system,
                common.uid user_id,
                common.vc  version_code
         from ods_log_inc
         where dt = '2020-06-15'
           and page.page_id = 'register'
           and common.uid is not null
     ) log
     on ui.user_id = log.user_id
         left join
     (
         select id province_id,
                area_code
         from ods_base_province_full
         where dt = '2020-06-15'
     ) bp
     on log.area_code = bp.area_code;

-- 用户域用户登录事务事实表
CREATE EXTERNAL TABLE dwd_user_login_inc
(
    `user_id`        STRING COMMENT '用户ID',
    `date_id`        STRING COMMENT '日期ID',
    `login_time`     STRING COMMENT '登录时间',
    `channel`        STRING COMMENT '应用下载渠道',
    `province_id`    STRING COMMENT '省份id',
    `version_code`   STRING COMMENT '应用版本',
    `mid_id`         STRING COMMENT '设备id',
    `brand`          STRING COMMENT '设备品牌',
    `model`          STRING COMMENT '设备型号',
    `operate_system` STRING COMMENT '设备操作系统'
) COMMENT '用户域用户登录事务事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dwd/dwd_user_login_inc/'
    TBLPROPERTIES ("orc.compress" = "snappy");

-- 数据装载
insert overwrite table dwd_user_login_inc partition (dt = '2020-06-14')
select user_id,
       date_format(from_utc_timestamp(ts, 'GMT+8'), 'yyyy-MM-dd')          date_id,
       date_format(from_utc_timestamp(ts, 'GMT+8'), 'yyyy-MM-dd HH:mm:ss') login_time,
       channel,
       province_id,
       version_code,
       mid_id,
       brand,
       model,
       operate_system
from (
         select user_id,
                channel,
                area_code,
                version_code,
                mid_id,
                brand,
                model,
                operate_system,
                ts
         from (
                  select user_id,
                         channel,
                         area_code,
                         version_code,
                         mid_id,
                         brand,
                         model,
                         operate_system,
                         ts,
                         row_number() over (partition by session_id order by ts) rn
                  from (
                           select user_id,
                                  channel,
                                  area_code,
                                  version_code,
                                  mid_id,
                                  brand,
                                  model,
                                  operate_system,
                                  ts,
                                  concat(mid_id, '-', last_value(session_start_point, true)
                                                                 over (partition by mid_id order by ts)) session_id
                           from (
                                    select common.uid                              user_id,
                                           common.ch                               channel,
                                           common.ar                               area_code,
                                           common.vc                               version_code,
                                           common.mid                              mid_id,
                                           common.ba                               brand,
                                           common.md                               model,
                                           common.os                               operate_system,
                                           ts,
                                           if(page.last_page_id is null, ts, null) session_start_point
                                    from ods_log_inc
                                    where dt = '2020-06-14'
                                      and page is not null
                                ) t1
                       ) t2
                  where user_id is not null
              ) t3
         where rn = 1
     ) t4
         left join
     (
         select id province_id,
                area_code
         from ods_base_province_full
         where dt = '2020-06-14'
     ) bp
     on t4.area_code = bp.area_code;

/*
DWS层设计要点:
（1）DWS层的设计参考指标体系,根据 ADS 层的各种指标进行设计，数据来源 DIM、DWD、DWS
（2）DWS层的数据存储格式为ORC列式存储 + snappy压缩
（3）DWS层表名的命名规范为dws_数据域_统计粒度_业务过程_统计周期（1d/nd/td）
注：1d表示最近1日，nd表示最近n日，td表示历史至今
*/

/*最近1、7、30日各品牌订单数*/
/*
create external table dws_trade_user_tm_order_1d
(
    `user_id`            string comment '用户id',
    `tm_id`              string comment '品牌id',
    `tm_name`            string comment '品牌名称',
    `order_count`        bigint comment '下单次数',
    `order_num`          bigint comment '下单件数',
    `order_total_amount` decimal(16, 2) comment '下单金额'
) comment '交易域用户品牌粒度订单最近1日汇总事实表'
    partitioned by (`dt` string)
    stored as orc
    location '/warehouse/gmall/dws/dws_trade_user_tm_order_1d'
    tblproperties ('orc.compess' = 'snappy');

-- 数据装载
insert overwrite table dws_trade_user_tm_order_1d partition (dt = '2020-06-14')
select user_id,
       tm_id,
       tm_name,
       count(*),
       sum(sku_num),
       sum(split_total_amount)
from (select sku_id,
             user_id,
             sku_num,
             split_total_amount
      from dwd_trade_order_detail_inc
      where dt = '2020-06-14'
     ) od
         left join
     (
         select id,
                tm_id,
                tm_name
         from dim_sku_full
         where dt = '2020-06-14'
     ) sku
     on od.sku_id = sku.id
group by user_id, tm_id, tm_name;

select tm_id,
       tm_name,
       sum(order_count),
       count(distinct (user_id))
from dws_trade_user_tm_order_1d
where dt = '2020-06-14'
group by tm_id, tm_name;

create external table dws_trade_user_tm_order_nd
(
    `user_id`                string comment '用户id',
    `tm_id`                  string comment '品牌id',
    `tm_name`                string comment '品牌名称',
    `order_count_7d`         bigint comment '最近7日下单次数',
    `order_num_7d`           bigint comment '最近7日下单件数',
    `order_total_amount_7d`  decimal(16, 2) comment '最近7日下单金额',
    `order_count_30d`        bigint comment '最近30日下单次数',
    `order_num_30d`          bigint comment '最近30日下单件数',
    `order_total_amount_30d` decimal(16, 2) comment '最近30日下单金额'
) comment '交易域用户品牌粒度订单最近7日汇总事实表'
    partitioned by (`dt` string)
    stored as orc
    location '/warehouse/gmall/dws/dws_trade_user_tm_order_nd'
    tblproperties ('orc.compess' = 'snappy');

-- 数据装载
insert overwrite table dws_trade_user_tm_order_nd partition (dt = '2020-06-14')
select user_id,
       tm_id,
       tm_name,
       sum(if(dt >= date_sub('2020-06-14', 6), order_count, 0)),
       sum(if(dt >= date_sub('2020-06-14', 6), order_num, 0)),
       sum(if(dt >= date_sub('2020-06-14', 6), order_total_amount, 0)),
       sum(order_count),
       sum(order_num),
       sum(order_total_amount)
from dws_trade_user_tm_order_1d
where dt >= date_sub(2020 - 06 - 14, 29)
group by tm_id, tm_name;

select tm_id,
       tm_name,
       sum(order_count_7d),
       count(distinct (if(order_count_7d > 0, user_id, null)))
from dws_trade_user_tm_order_nd
where dt = '2020-06-14'
group by tm_id, tm_name;

*/
/*最近1、7、30日各品类订单数*//*

create external table dws_trade_user_cate_order_1d
(
    `user_id`            string comment '用户id',
    `cate1_id`           string comment '一级分类id',
    `cate1_name`         string comment '一级分类名称',
    `cate2_id`           string comment '二级分类id',
    `cate2_name`         string comment '二级分类名称',
    `cate3_id`           string comment '三级分类id',
    `cate3_name`         string comment '三级分类名称',
    `order_count`        bigint comment '下单次数',
    `order_num`          bigint comment '下单件数',
    `order_total_amount` decimal(16, 2) comment '下单金额'
) comment '交易域用户品类粒度订单最近1日汇总事实表'
    partitioned by (`dt` string)
    stored as orc
    location '/warehouse/gmall/dws/dws_trade_user_cate_order_1d'
    tblproperties ('orc.compess' = 'snappy');

create external table dws_trade_user_cate_order_nd
(
    `user_id`                string comment '用户id',
    `cate1_id`               string comment '一级分类id',
    `cate1_name`             string comment '一级分类名称',
    `cate2_id`               string comment '二级分类id',
    `cate2_name`             string comment '二级分类名称',
    `cate3_id`               string comment '三级分类id',
    `cate3_name`             string comment '三级分类名称',
    `order_count_7d`         bigint comment '最近7日下单次数',
    `order_num_7d`           bigint comment '最近7日下单件数',
    `order_total_amount_7d`  decimal(16, 2) comment '最近7日下单金额',
    `order_count_30d`        bigint comment '最近30日下单次数',
    `order_num_30d`          bigint comment '最近30日下单件数',
    `order_total_amount_30d` decimal(16, 2) comment '最近30日下单金额'
) comment '交易域用户品类粒度订单最近7日汇总事实表'
    partitioned by (`dt` string)
    stored as orc
    location '/warehouse/gmall/dws/dws_trade_user_cate_order_nd'
    tblproperties ('orc.compess' = 'snappy');

*/
/*最近1、7、30日各商品订单数，品牌和品类的合并，最终方案*//*

create external table dws_trade_user_sku_order_1d
(
    `user_id`            string comment '用户id',
    `sku_id`             string comment '商品id',
    `tm_id`              string comment '品牌id',
    `tm_name`            string comment '品牌名称',
    `cate1_id`           string comment '一级分类id',
    `cate1_name`         string comment '一级分类名称',
    `cate2_id`           string comment '二级分类id',
    `cate2_name`         string comment '二级分类名称',
    `cate3_id`           string comment '三级分类id',
    `cate3_name`         string comment '三级分类名称',
    `order_count`        bigint comment '下单次数',
    `order_num`          bigint comment '下单件数',
    `order_total_amount` decimal(16, 2) comment '下单金额'
) comment '交易域用户商品粒度订单最近1日汇总事实表'
    partitioned by (`dt` string)
    stored as orc
    location '/warehouse/gmall/dws/dws_trade_user_sku_order_1d'
    tblproperties ('orc.compess' = 'snappy');

create external table dws_trade_user_sku_order_nd
(
    `user_id`                string comment '用户id',
    `sku_id`                 string comment '商品id',
    `tm_id`                  string comment '品牌id',
    `tm_name`                string comment '品牌名称',
    `cate1_id`               string comment '一级分类id',
    `cate1_name`             string comment '一级分类名称',
    `cate2_id`               string comment '二级分类id',
    `cate2_name`             string comment '二级分类名称',
    `cate3_id`               string comment '三级分类id',
    `cate3_name`             string comment '三级分类名称',
    `order_count_7d`         bigint comment '最近7日下单次数',
    `order_num_7d`           bigint comment '最近7日下单件数',
    `order_total_amount_7d`  decimal(16, 2) comment '最近7日下单金额',
    `order_count_30d`        bigint comment '最近30日下单次数',
    `order_num_30d`          bigint comment '最近30日下单件数',
    `order_total_amount_30d` decimal(16, 2) comment '最近30日下单金额'
) comment '交易域用户商品粒度订单最近7日汇总事实表'
    partitioned by (`dt` string)
    stored as orc
    location '/warehouse/gmall/dws/dws_trade_user_sku_order_nd'
    tblproperties ('orc.compess' = 'snappy');

*/
/*历史表至今-汇总表分析*//*

create external table dws_trade_user_order_td
(
    `user_id`            string comment '用户id',
    `order_date_first`   string comment '首次下单日期',
    `order_date_last`    string comment '最后一次下单日期',
    `order_count`        bigint comment '下单次数',
    `order_num`          bigint comment '下单件数',
    `order_total_amount` decimal(16, 2) comment '下单金额'
) comment '交易域用户粒度订单最近7日汇总事实表'
    partitioned by (`dt` string)
    stored as orc
    location '/warehouse/gmall/dws/dws_trade_user_order_nd'
    tblproperties ('orc.compess' = 'snappy');

-- 数据装载，首日装载
insert overwrite table dws_trade_user_order_td partition (dt = '2020-06-14')
select user_id,
       min(date_id),
       max(date_id),
       count(distinct (order_id)),
       sum(sku_num),
       sum(split_total_amount)
from dwd_trade_order_detail_inc
group by user_id;

-- 每日装载
-- 方案一
select nvl(new.user_id, old.user_id),
       if(old.user_id is null, '2020-06-15', old.order_date_first),
       if(new.user_id is null, old.order_date_first, '2020-06-15'),
--     nvl() : 空值的转换,如果第一个参数的值为空值，则返回第二个参数的值，否则返回第一个参数的值
       nvl(order_count_td, 0) + nvl(order_count_1d, 0),
       nvl(order_num_td, 0) + nvl(order_num_1d, 0),
       nvl(order_total_amount_td, 0) + nvl(order_total_amount_1d, 0)
from (select user_id,
             order_date_first,
             order_date_last,
             order_count        order_count_td,
             order_num          order_num_td,
             order_total_amount order_total_amount_td
      from dws_trade_user_order_td
      where dt = date_sub('2020-06-15', 1)
     ) old
         full outer join
     (select user_id,
             count(distinct (order_id)) order_count_1d,
             sum(sku_num)               order_num_1d,
             sum(split_total_amount)    order_total_amount_1d
      from dwd_trade_order_detail_inc
      where dt = '2020-06-15'
      group by user_id) new
     on old.user_id = new.user_id;

-- 每日装载
-- 方案二
select user_id,
       min(order_date_first),
       max(order_date_last),
       sum(order_count_td),
       sum(order_num_td),
       sum(order_total_amount_td)
from (
         select user_id,
                order_date_first,
                order_date_last,
                order_count        order_count_td,
                order_num          order_num_td,
                order_total_amount order_total_amount_td
         from dws_trade_user_order_td
         where dt = date_sub('2020-06-15', 1)
         union all
         select user_id,
                '2020-06-15',
                '2020-06-15',
                count(distinct (order_id)) order_count_1d,
                sum(sku_num)               order_num_1d,
                sum(split_total_amount)    order_total_amount_1d
         from dwd_trade_order_detail_inc
         where dt = '2020-06-15'
         group by user_id
     ) t1
group by user_id;
*/

-- 交易域用户商品粒度订单最近1日汇总表
CREATE EXTERNAL TABLE dws_trade_user_sku_order_1d
(
    `user_id`                   STRING COMMENT '用户id',
    `sku_id`                    STRING COMMENT 'sku_id',
    `sku_name`                  STRING COMMENT 'sku名称',
    `category1_id`              STRING COMMENT '一级分类id',
    `category1_name`            STRING COMMENT '一级分类名称',
    `category2_id`              STRING COMMENT '一级分类id',
    `category2_name`            STRING COMMENT '一级分类名称',
    `category3_id`              STRING COMMENT '一级分类id',
    `category3_name`            STRING COMMENT '一级分类名称',
    `tm_id`                     STRING COMMENT '品牌id',
    `tm_name`                   STRING COMMENT '品牌名称',
    `order_count_1d`            BIGINT COMMENT '最近1日下单次数',
    `order_num_1d`              BIGINT COMMENT '最近1日下单件数',
    `order_original_amount_1d`  DECIMAL(16, 2) COMMENT '最近1日下单原始金额',
    `activity_reduce_amount_1d` DECIMAL(16, 2) COMMENT '最近1日活动优惠金额',
    `coupon_reduce_amount_1d`   DECIMAL(16, 2) COMMENT '最近1日优惠券优惠金额',
    `order_total_amount_1d`     DECIMAL(16, 2) COMMENT '最近1日下单最终金额'
) COMMENT '交易域用户商品粒度订单最近1日汇总事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dws/dws_trade_user_sku_order_1d'
    TBLPROPERTIES ('orc.compress' = 'snappy');

-- 数据装载，首日装载
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dws_trade_user_sku_order_1d partition (dt)
select user_id,
       id,
       sku_name,
       category1_id,
       category1_name,
       category2_id,
       category2_name,
       category3_id,
       category3_name,
       tm_id,
       tm_name,
       order_count_1d,
       order_num_1d,
       order_original_amount_1d,
       activity_reduce_amount_1d,
       coupon_reduce_amount_1d,
       order_total_amount_1d,
       dt
from (
         select dt,
                user_id,
                sku_id,
                count(*)                             order_count_1d,
                sum(sku_num)                         order_num_1d,
                sum(split_original_amount)           order_original_amount_1d,
                sum(nvl(split_activity_amount, 0.0)) activity_reduce_amount_1d,
                sum(nvl(split_coupon_amount, 0.0))   coupon_reduce_amount_1d,
                sum(split_total_amount)              order_total_amount_1d
         from dwd_trade_order_detail_inc
         group by dt, user_id, sku_id
     ) od
         left join
     (
         select id,
                sku_name,
                category1_id,
                category1_name,
                category2_id,
                category2_name,
                category3_id,
                category3_name,
                tm_id,
                tm_name
         from dim_sku_full
         where dt = '2020-06-14'
     ) sku
     on od.sku_id = sku.id;

-- 每日装载
insert overwrite table dws_trade_user_sku_order_1d partition (dt = '2020-06-15')
select user_id,
       id,
       sku_name,
       category1_id,
       category1_name,
       category2_id,
       category2_name,
       category3_id,
       category3_name,
       tm_id,
       tm_name,
       order_count,
       order_num,
       order_original_amount,
       activity_reduce_amount,
       coupon_reduce_amount,
       order_total_amount
from (
         select user_id,
                sku_id,
                count(*)                           order_count,
                sum(sku_num)                       order_num,
                sum(split_original_amount)         order_original_amount,
                sum(nvl(split_activity_amount, 0)) activity_reduce_amount,
                sum(nvl(split_coupon_amount, 0))   coupon_reduce_amount,
                sum(split_total_amount)            order_total_amount
         from dwd_trade_order_detail_inc
         where dt = '2020-06-15'
         group by user_id, sku_id
     ) od
         left join
     (
         select id,
                sku_name,
                category1_id,
                category1_name,
                category2_id,
                category2_name,
                category3_id,
                category3_name,
                tm_id,
                tm_name
         from dim_sku_full
         where dt = '2020-06-15'
     ) sku
     on od.sku_id = sku.id;

-- 交易域用户商品粒度订单最近n日汇总表
CREATE EXTERNAL TABLE dws_trade_user_sku_order_nd
(
    `user_id`                    STRING COMMENT '用户id',
    `sku_id`                     STRING COMMENT 'sku_id',
    `sku_name`                   STRING COMMENT 'sku名称',
    `category1_id`               STRING COMMENT '一级分类id',
    `category1_name`             STRING COMMENT '一级分类名称',
    `category2_id`               STRING COMMENT '一级分类id',
    `category2_name`             STRING COMMENT '一级分类名称',
    `category3_id`               STRING COMMENT '一级分类id',
    `category3_name`             STRING COMMENT '一级分类名称',
    `tm_id`                      STRING COMMENT '品牌id',
    `tm_name`                    STRING COMMENT '品牌名称',
    `order_count_7d`             STRING COMMENT '最近7日下单次数',
    `order_num_7d`               BIGINT COMMENT '最近7日下单件数',
    `order_original_amount_7d`   DECIMAL(16, 2) COMMENT '最近7日下单原始金额',
    `activity_reduce_amount_7d`  DECIMAL(16, 2) COMMENT '最近7日活动优惠金额',
    `coupon_reduce_amount_7d`    DECIMAL(16, 2) COMMENT '最近7日优惠券优惠金额',
    `order_total_amount_7d`      DECIMAL(16, 2) COMMENT '最近7日下单最终金额',
    `order_count_30d`            BIGINT COMMENT '最近30日下单次数',
    `order_num_30d`              BIGINT COMMENT '最近30日下单件数',
    `order_original_amount_30d`  DECIMAL(16, 2) COMMENT '最近30日下单原始金额',
    `activity_reduce_amount_30d` DECIMAL(16, 2) COMMENT '最近30日活动优惠金额',
    `coupon_reduce_amount_30d`   DECIMAL(16, 2) COMMENT '最近30日优惠券优惠金额',
    `order_total_amount_30d`     DECIMAL(16, 2) COMMENT '最近30日下单最终金额'
) COMMENT '交易域用户商品粒度订单最近n日汇总事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dws/dws_trade_user_sku_order_nd'
    TBLPROPERTIES ('orc.compress' = 'snappy');

-- 数据装载
insert overwrite table dws_trade_user_sku_order_nd partition (dt = '2020-06-14')
select user_id,
       sku_id,
       sku_name,
       category1_id,
       category1_name,
       category2_id,
       category2_name,
       category3_id,
       category3_name,
       tm_id,
       tm_name,
       sum(if(dt >= date_add('2020-06-14', -6), order_count_1d, 0)),
       sum(if(dt >= date_add('2020-06-14', -6), order_num_1d, 0)),
       sum(if(dt >= date_add('2020-06-14', -6), order_original_amount_1d, 0)),
       sum(if(dt >= date_add('2020-06-14', -6), activity_reduce_amount_1d, 0)),
       sum(if(dt >= date_add('2020-06-14', -6), coupon_reduce_amount_1d, 0)),
       sum(if(dt >= date_add('2020-06-14', -6), order_total_amount_1d, 0)),
       sum(order_count_1d),
       sum(order_num_1d),
       sum(order_original_amount_1d),
       sum(activity_reduce_amount_1d),
       sum(coupon_reduce_amount_1d),
       sum(order_total_amount_1d)
from dws_trade_user_sku_order_1d
where dt >= date_add('2020-06-14', -29)
group by user_id, sku_id, sku_name, category1_id, category1_name, category2_id, category2_name, category3_id,
         category3_name, tm_id, tm_name;

-- 交易域用户商品粒度退单最近1日汇总表
CREATE EXTERNAL TABLE dws_trade_user_sku_order_refund_1d
(
    `user_id`                STRING COMMENT '用户id',
    `sku_id`                 STRING COMMENT 'sku_id',
    `sku_name`               STRING COMMENT 'sku名称',
    `category1_id`           STRING COMMENT '一级分类id',
    `category1_name`         STRING COMMENT '一级分类名称',
    `category2_id`           STRING COMMENT '一级分类id',
    `category2_name`         STRING COMMENT '一级分类名称',
    `category3_id`           STRING COMMENT '一级分类id',
    `category3_name`         STRING COMMENT '一级分类名称',
    `tm_id`                  STRING COMMENT '品牌id',
    `tm_name`                STRING COMMENT '品牌名称',
    `order_refund_count_1d`  BIGINT COMMENT '最近1日退单次数',
    `order_refund_num_1d`    BIGINT COMMENT '最近1日退单件数',
    `order_refund_amount_1d` DECIMAL(16, 2) COMMENT '最近1日退单金额'
) COMMENT '交易域用户商品粒度退单最近1日汇总事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dws/dws_trade_user_sku_order_refund_1d'
    TBLPROPERTIES ('orc.compress' = 'snappy');

-- 首日装载
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dws_trade_user_sku_order_refund_1d partition (dt)
select user_id,
       sku_id,
       sku_name,
       category1_id,
       category1_name,
       category2_id,
       category2_name,
       category3_id,
       category3_name,
       tm_id,
       tm_name,
       order_refund_count,
       order_refund_num,
       order_refund_amount,
       dt
from (
         select dt,
                user_id,
                sku_id,
                count(*)           order_refund_count,
                sum(refund_num)    order_refund_num,
                sum(refund_amount) order_refund_amount
         from dwd_trade_order_refund_inc
         group by dt, user_id, sku_id
     ) od
         left join
     (
         select id,
                sku_name,
                category1_id,
                category1_name,
                category2_id,
                category2_name,
                category3_id,
                category3_name,
                tm_id,
                tm_name
         from dim_sku_full
         where dt = '2020-06-14'
     ) sku
     on od.sku_id = sku.id;

-- 每日装载
insert overwrite table dws_trade_user_sku_order_refund_1d partition (dt = '2020-06-15')
select user_id,
       sku_id,
       sku_name,
       category1_id,
       category1_name,
       category2_id,
       category2_name,
       category3_id,
       category3_name,
       tm_id,
       tm_name,
       order_refund_count,
       order_refund_num,
       order_refund_amount
from (
         select user_id,
                sku_id,
                count(*)           order_refund_count,
                sum(refund_num)    order_refund_num,
                sum(refund_amount) order_refund_amount
         from dwd_trade_order_refund_inc
         where dt = '2020-06-15'
         group by user_id, sku_id
     ) od
         left join
     (
         select id,
                sku_name,
                category1_id,
                category1_name,
                category2_id,
                category2_name,
                category3_id,
                category3_name,
                tm_id,
                tm_name
         from dim_sku_full
         where dt = '2020-06-15'
     ) sku
     on od.sku_id = sku.id;

-- 交易域用户商品粒度退单最近n日汇总表
CREATE EXTERNAL TABLE dws_trade_user_sku_order_refund_nd
(
    `user_id`                 STRING COMMENT '用户id',
    `sku_id`                  STRING COMMENT 'sku_id',
    `sku_name`                STRING COMMENT 'sku名称',
    `category1_id`            STRING COMMENT '一级分类id',
    `category1_name`          STRING COMMENT '一级分类名称',
    `category2_id`            STRING COMMENT '一级分类id',
    `category2_name`          STRING COMMENT '一级分类名称',
    `category3_id`            STRING COMMENT '一级分类id',
    `category3_name`          STRING COMMENT '一级分类名称',
    `tm_id`                   STRING COMMENT '品牌id',
    `tm_name`                 STRING COMMENT '品牌名称',
    `order_refund_count_7d`   BIGINT COMMENT '最近7日退单次数',
    `order_refund_num_7d`     BIGINT COMMENT '最近7日退单件数',
    `order_refund_amount_7d`  DECIMAL(16, 2) COMMENT '最近7日退单金额',
    `order_refund_count_30d`  BIGINT COMMENT '最近30日退单次数',
    `order_refund_num_30d`    BIGINT COMMENT '最近30日退单件数',
    `order_refund_amount_30d` DECIMAL(16, 2) COMMENT '最近30日退单金额'
) COMMENT '交易域用户商品粒度退单最近n日汇总事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dws/dws_trade_user_sku_order_refund_nd'
    TBLPROPERTIES ('orc.compress' = 'snappy');

-- 数据装载
insert overwrite table dws_trade_user_sku_order_refund_nd partition (dt = '2020-06-14')
select user_id,
       sku_id,
       sku_name,
       category1_id,
       category1_name,
       category2_id,
       category2_name,
       category3_id,
       category3_name,
       tm_id,
       tm_name,
       sum(if(dt >= date_add('2020-06-14', -6), order_refund_count_1d, 0)),
       sum(if(dt >= date_add('2020-06-14', -6), order_refund_num_1d, 0)),
       sum(if(dt >= date_add('2020-06-14', -6), order_refund_amount_1d, 0)),
       sum(order_refund_count_1d),
       sum(order_refund_num_1d),
       sum(order_refund_amount_1d)
from dws_trade_user_sku_order_refund_1d
where dt >= date_add('2020-06-14', -29)
  and dt <= '2020-06-14'
group by user_id, sku_id, sku_name, category1_id, category1_name, category2_id, category2_name, category3_id,
         category3_name, tm_id, tm_name;

-- 交易域用户粒度订单最近1日汇总表
CREATE EXTERNAL TABLE dws_trade_user_order_1d
(
    `user_id`                   STRING COMMENT '用户id',
    `order_count_1d`            BIGINT COMMENT '最近1日下单次数',
    `order_num_1d`              BIGINT COMMENT '最近1日下单商品件数',
    `order_original_amount_1d`  DECIMAL(16, 2) COMMENT '最近1日最近1日下单原始金额',
    `activity_reduce_amount_1d` DECIMAL(16, 2) COMMENT '最近1日下单活动优惠金额',
    `coupon_reduce_amount_1d`   DECIMAL(16, 2) COMMENT '下单优惠券优惠金额',
    `order_total_amount_1d`     DECIMAL(16, 2) COMMENT '最近1日下单最终金额'
) COMMENT '交易域用户粒度订单最近1日汇总事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dws/dws_trade_user_order_1d'
    TBLPROPERTIES ('orc.compress' = 'snappy');

-- 首日装载
insert overwrite table dws_trade_user_order_1d partition (dt)
select user_id,
       count(distinct (order_id)),
       sum(sku_num),
       sum(split_original_amount),
       sum(nvl(split_activity_amount, 0)),
       sum(nvl(split_coupon_amount, 0)),
       sum(split_total_amount),
       dt
from dwd_trade_order_detail_inc
group by user_id, dt;

-- 每日装载
insert overwrite table dws_trade_user_order_1d partition (dt = '2020-06-15')
select user_id,
       count(distinct (order_id)),
       sum(sku_num),
       sum(split_original_amount),
       sum(nvl(split_activity_amount, 0)),
       sum(nvl(split_coupon_amount, 0)),
       sum(split_total_amount)
from dwd_trade_order_detail_inc
where dt = '2020-06-15'
group by user_id;

-- 交易域用户粒度订单最近n日汇总表
CREATE EXTERNAL TABLE dws_trade_user_order_nd
(
    `user_id`                    STRING COMMENT '用户id',
    `order_count_7d`             BIGINT COMMENT '最近7日下单次数',
    `order_num_7d`               BIGINT COMMENT '最近7日下单商品件数',
    `order_original_amount_7d`   DECIMAL(16, 2) COMMENT '最近7日下单原始金额',
    `activity_reduce_amount_7d`  DECIMAL(16, 2) COMMENT '最近7日下单活动优惠金额',
    `coupon_reduce_amount_7d`    DECIMAL(16, 2) COMMENT '最近7日下单优惠券优惠金额',
    `order_total_amount_7d`      DECIMAL(16, 2) COMMENT '最近7日下单最终金额',
    `order_count_30d`            BIGINT COMMENT '最近30日下单次数',
    `order_num_30d`              BIGINT COMMENT '最近30日下单商品件数',
    `order_original_amount_30d`  DECIMAL(16, 2) COMMENT '最近30日下单原始金额',
    `activity_reduce_amount_30d` DECIMAL(16, 2) COMMENT '最近30日下单活动优惠金额',
    `coupon_reduce_amount_30d`   DECIMAL(16, 2) COMMENT '最近30日下单优惠券优惠金额',
    `order_total_amount_30d`     DECIMAL(16, 2) COMMENT '最近30日下单最终金额'
) COMMENT '交易域用户粒度订单最近n日汇总事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dws/dws_trade_user_order_nd'
    TBLPROPERTIES ('orc.compress' = 'snappy');

-- 数据装载
insert overwrite table dws_trade_user_order_nd partition (dt = '2020-06-14')
select user_id,
       sum(if(dt >= date_add('2020-06-14', -6), order_count_1d, 0)),
       sum(if(dt >= date_add('2020-06-14', -6), order_num_1d, 0)),
       sum(if(dt >= date_add('2020-06-14', -6), order_original_amount_1d, 0)),
       sum(if(dt >= date_add('2020-06-14', -6), activity_reduce_amount_1d, 0)),
       sum(if(dt >= date_add('2020-06-14', -6), coupon_reduce_amount_1d, 0)),
       sum(if(dt >= date_add('2020-06-14', -6), order_total_amount_1d, 0)),
       sum(order_count_1d),
       sum(order_num_1d),
       sum(order_original_amount_1d),
       sum(activity_reduce_amount_1d),
       sum(coupon_reduce_amount_1d),
       sum(order_total_amount_1d)
from dws_trade_user_order_1d
where dt >= date_add('2020-06-14', -29)
  and dt <= '2020-06-14'
group by user_id;

-- 交易域用户粒度订单历史至今汇总表
CREATE EXTERNAL TABLE dws_trade_user_order_td
(
    `user_id`                   STRING COMMENT '用户id',
    `order_date_first`          STRING COMMENT '首次下单日期',
    `order_date_last`           STRING COMMENT '末次下单日期',
    `order_count_td`            BIGINT COMMENT '下单次数',
    `order_num_td`              BIGINT COMMENT '购买商品件数',
    `original_amount_td`        DECIMAL(16, 2) COMMENT '原始金额',
    `activity_reduce_amount_td` DECIMAL(16, 2) COMMENT '活动优惠金额',
    `coupon_reduce_amount_td`   DECIMAL(16, 2) COMMENT '优惠券优惠金额',
    `total_amount_td`           DECIMAL(16, 2) COMMENT '最终金额'
) COMMENT '交易域用户粒度订单历史至今汇总事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dws/dws_trade_user_order_td'
    TBLPROPERTIES ('orc.compress' = 'snappy');

-- 首日装载
insert overwrite table dws_trade_user_order_td partition (dt = '2020-06-14')
select user_id,
       min(dt)                        login_date_first,
       max(dt)                        login_date_last,
       sum(order_count_1d)            order_count,
       sum(order_num_1d)              order_num,
       sum(order_original_amount_1d)  original_amount,
       sum(activity_reduce_amount_1d) activity_reduce_amount,
       sum(coupon_reduce_amount_1d)   coupon_reduce_amount,
       sum(order_total_amount_1d)     total_amount
from dws_trade_user_order_1d
group by user_id;

-- 每日装载
insert overwrite table dws_trade_user_order_td partition (dt = '2020-06-15')
select nvl(old.user_id, new.user_id),
       if(new.user_id is not null and old.user_id is null, '2020-06-15', old.order_date_first),
       if(new.user_id is not null, '2020-06-15', old.order_date_last),
       nvl(old.order_count_td, 0) + nvl(new.order_count_1d, 0),
       nvl(old.order_num_td, 0) + nvl(new.order_num_1d, 0),
       nvl(old.original_amount_td, 0) + nvl(new.order_original_amount_1d, 0),
       nvl(old.activity_reduce_amount_td, 0) + nvl(new.activity_reduce_amount_1d, 0),
       nvl(old.coupon_reduce_amount_td, 0) + nvl(new.coupon_reduce_amount_1d, 0),
       nvl(old.total_amount_td, 0) + nvl(new.order_total_amount_1d, 0)
from (
         select user_id,
                order_date_first,
                order_date_last,
                order_count_td,
                order_num_td,
                original_amount_td,
                activity_reduce_amount_td,
                coupon_reduce_amount_td,
                total_amount_td
         from dws_trade_user_order_td
         where dt = date_add('2020-06-15', -1)
     ) old
         full outer join
     (
         select user_id,
                order_count_1d,
                order_num_1d,
                order_original_amount_1d,
                activity_reduce_amount_1d,
                coupon_reduce_amount_1d,
                order_total_amount_1d
         from dws_trade_user_order_1d
         where dt = '2020-06-15'
     ) new
     on old.user_id = new.user_id;

-- 交易域用户粒度加购最近1日汇总表
CREATE EXTERNAL TABLE dws_trade_user_cart_add_1d
(
    `user_id`           STRING COMMENT '用户id',
    `cart_add_count_1d` BIGINT COMMENT '最近1日加购次数',
    `cart_add_num_1d`   BIGINT COMMENT '最近1日加购商品件数'
) COMMENT '交易域用户粒度加购最近1日汇总事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dws/dws_trade_user_cart_add_1d'
    TBLPROPERTIES ('orc.compress' = 'snappy');

-- 首日装载
insert overwrite table dws_trade_user_cart_add_1d partition (dt)
select user_id,
       count(*),
       sum(sku_num),
       dt
from dwd_trade_cart_add_inc
group by user_id, dt;

-- 每日装载
insert overwrite table dws_trade_user_cart_add_1d partition (dt = '2020-06-15')
select user_id,
       count(*),
       sum(sku_num)
from dwd_trade_cart_add_inc
where dt = '2020-06-15'
group by user_id;

-- 交易域用户粒度支付最近1日汇总表
CREATE EXTERNAL TABLE dws_trade_user_payment_1d
(
    `user_id`           STRING COMMENT '用户id',
    `payment_count_1d`  BIGINT COMMENT '最近1日支付次数',
    `payment_num_1d`    BIGINT COMMENT '最近1日支付商品件数',
    `payment_amount_1d` DECIMAL(16, 2) COMMENT '最近1日支付金额'
) COMMENT '交易域用户粒度支付最近1日汇总事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dws/dws_trade_user_payment_1d'
    TBLPROPERTIES ('orc.compress' = 'snappy');

-- 首日装载
insert overwrite table dws_trade_user_payment_1d partition (dt)
select user_id,
       count(distinct (order_id)),
       sum(sku_num),
       sum(split_payment_amount),
       dt
from dwd_trade_pay_detail_suc_inc
group by user_id, dt;

-- 每日装载
insert overwrite table dws_trade_user_payment_1d partition (dt = '2020-06-15')
select user_id,
       count(distinct (order_id)),
       sum(sku_num),
       sum(split_payment_amount)
from dwd_trade_pay_detail_suc_inc
where dt = '2020-06-15'
group by user_id;

-- 交易域用户粒度支付最近n日汇总表
CREATE EXTERNAL TABLE dws_trade_user_payment_nd
(
    `user_id`            STRING COMMENT '用户id',
    `payment_count_7d`   BIGINT COMMENT '最近7日支付次数',
    `payment_num_7d`     BIGINT COMMENT '最近7日支付商品件数',
    `payment_amount_7d`  DECIMAL(16, 2) COMMENT '最近7日支付金额',
    `payment_count_30d`  BIGINT COMMENT '最近30日支付次数',
    `payment_num_30d`    BIGINT COMMENT '最近30日支付商品件数',
    `payment_amount_30d` DECIMAL(16, 2) COMMENT '最近30日支付金额'
) COMMENT '交易域用户粒度支付最近n日汇总事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dws/dws_trade_user_payment_nd'
    TBLPROPERTIES ('orc.compress' = 'snappy');

-- 数据装载
insert overwrite table dws_trade_user_payment_nd partition (dt = '2020-06-14')
select user_id,
       sum(if(dt >= date_add('2020-06-14', -6), payment_count_1d, 0)),
       sum(if(dt >= date_add('2020-06-14', -6), payment_num_1d, 0)),
       sum(if(dt >= date_add('2020-06-14', -6), payment_amount_1d, 0)),
       sum(payment_count_1d),
       sum(payment_num_1d),
       sum(payment_amount_1d)
from dws_trade_user_payment_1d
where dt >= date_add('2020-06-14', -29)
  and dt <= '2020-06-14'
group by user_id;

-- 交易域用户粒度支付历史至今汇总表
CREATE EXTERNAL TABLE dws_trade_user_payment_td
(
    `user_id`            STRING COMMENT '用户id',
    `payment_date_first` STRING COMMENT '首次支付日期',
    `payment_date_last`  STRING COMMENT '末次支付日期',
    `payment_count_td`   BIGINT COMMENT '最近7日支付次数',
    `payment_num_td`     BIGINT COMMENT '最近7日支付商品件数',
    `payment_amount_td`  DECIMAL(16, 2) COMMENT '最近7日支付金额'
) COMMENT '交易域用户粒度支付历史至今汇总事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dws/dws_trade_user_payment_td'
    TBLPROPERTIES ('orc.compress' = 'snappy');

-- 首日装载
insert overwrite table dws_trade_user_payment_td partition (dt = '2020-06-14')
select user_id,
       min(dt)                payment_date_first,
       max(dt)                payment_date_last,
       sum(payment_count_1d)  payment_count,
       sum(payment_num_1d)    payment_num,
       sum(payment_amount_1d) payment_amount
from dws_trade_user_payment_1d
group by user_id;

-- 每日装载
insert overwrite table dws_trade_user_payment_td partition (dt = '2020-06-15')
select nvl(old.user_id, new.user_id),
       if(old.user_id is null and new.user_id is not null, '2020-06-15', old.payment_date_first),
       if(new.user_id is not null, '2020-06-15', old.payment_date_last),
       nvl(old.payment_count_td, 0) + nvl(new.payment_count_1d, 0),
       nvl(old.payment_num_td, 0) + nvl(new.payment_num_1d, 0),
       nvl(old.payment_amount_td, 0) + nvl(new.payment_amount_1d, 0)
from (
         select user_id,
                payment_date_first,
                payment_date_last,
                payment_count_td,
                payment_num_td,
                payment_amount_td
         from dws_trade_user_payment_td
         where dt = date_add('2020-06-15', -1)
     ) old
         full outer join
     (
         select user_id,
                payment_count_1d,
                payment_num_1d,
                payment_amount_1d
         from dws_trade_user_payment_1d
         where dt = '2020-06-15'
     ) new
     on old.user_id = new.user_id;

-- 交易域省份粒度订单最近1日汇总表
CREATE EXTERNAL TABLE dws_trade_province_order_1d
(
    `province_id`               STRING COMMENT '用户id',
    `province_name`             STRING COMMENT '省份名称',
    `area_code`                 STRING COMMENT '地区编码',
    `iso_code`                  STRING COMMENT '旧版ISO-3166-2编码',
    `iso_3166_2`                STRING COMMENT '新版版ISO-3166-2编码',
    `order_count_1d`            BIGINT COMMENT '最近1日下单次数',
    `order_original_amount_1d`  DECIMAL(16, 2) COMMENT '最近1日下单原始金额',
    `activity_reduce_amount_1d` DECIMAL(16, 2) COMMENT '最近1日下单活动优惠金额',
    `coupon_reduce_amount_1d`   DECIMAL(16, 2) COMMENT '最近1日下单优惠券优惠金额',
    `order_total_amount_1d`     DECIMAL(16, 2) COMMENT '最近1日下单最终金额'
) COMMENT '交易域省份粒度订单最近1日汇总事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dws/dws_trade_province_order_1d'
    TBLPROPERTIES ('orc.compress' = 'snappy');

-- 首日装载
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dws_trade_province_order_1d partition (dt)
select province_id,
       province_name,
       area_code,
       iso_code,
       iso_3166_2,
       order_count_1d,
       order_original_amount_1d,
       activity_reduce_amount_1d,
       coupon_reduce_amount_1d,
       order_total_amount_1d,
       dt
from (
         select province_id,
                count(distinct (order_id))         order_count_1d,
                sum(split_original_amount)         order_original_amount_1d,
                sum(nvl(split_activity_amount, 0)) activity_reduce_amount_1d,
                sum(nvl(split_coupon_amount, 0))   coupon_reduce_amount_1d,
                sum(split_total_amount)            order_total_amount_1d,
                dt
         from dwd_trade_order_detail_inc
         group by province_id, dt
     ) o
         left join
     (
         select id,
                province_name,
                area_code,
                iso_code,
                iso_3166_2
         from dim_province_full
         where dt = '2020-06-14'
     ) p
     on o.province_id = p.id;

-- 每日装载
insert overwrite table dws_trade_province_order_1d partition (dt = '2020-06-15')
select province_id,
       province_name,
       area_code,
       iso_code,
       iso_3166_2,
       order_count_1d,
       order_original_amount_1d,
       activity_reduce_amount_1d,
       coupon_reduce_amount_1d,
       order_total_amount_1d
from (
         select province_id,
                count(distinct (order_id))         order_count_1d,
                sum(split_original_amount)         order_original_amount_1d,
                sum(nvl(split_activity_amount, 0)) activity_reduce_amount_1d,
                sum(nvl(split_coupon_amount, 0))   coupon_reduce_amount_1d,
                sum(split_total_amount)            order_total_amount_1d
         from dwd_trade_order_detail_inc
         where dt = '2020-06-15'
         group by province_id
     ) o
         left join
     (
         select id,
                province_name,
                area_code,
                iso_code,
                iso_3166_2
         from dim_province_full
         where dt = '2020-06-15'
     ) p
     on o.province_id = p.id;

-- 交易域省份粒度订单最近n日汇总表
CREATE EXTERNAL TABLE dws_trade_province_order_nd
(
    `province_id`                STRING COMMENT '用户id',
    `province_name`              STRING COMMENT '省份名称',
    `area_code`                  STRING COMMENT '地区编码',
    `iso_code`                   STRING COMMENT '旧版ISO-3166-2编码',
    `iso_3166_2`                 STRING COMMENT '新版版ISO-3166-2编码',
    `order_count_7d`             BIGINT COMMENT '最近7日下单次数',
    `order_original_amount_7d`   DECIMAL(16, 2) COMMENT '最近7日下单原始金额',
    `activity_reduce_amount_7d`  DECIMAL(16, 2) COMMENT '最近7日下单活动优惠金额',
    `coupon_reduce_amount_7d`    DECIMAL(16, 2) COMMENT '最近7日下单优惠券优惠金额',
    `order_total_amount_7d`      DECIMAL(16, 2) COMMENT '最近7日下单最终金额',
    `order_count_30d`            BIGINT COMMENT '最近30日下单次数',
    `order_original_amount_30d`  DECIMAL(16, 2) COMMENT '最近30日下单原始金额',
    `activity_reduce_amount_30d` DECIMAL(16, 2) COMMENT '最近30日下单活动优惠金额',
    `coupon_reduce_amount_30d`   DECIMAL(16, 2) COMMENT '最近30日下单优惠券优惠金额',
    `order_total_amount_30d`     DECIMAL(16, 2) COMMENT '最近30日下单最终金额'
) COMMENT '交易域省份粒度订单最近n日汇总事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dws/dws_trade_province_order_nd'
    TBLPROPERTIES ('orc.compress' = 'snappy');

-- 数据装载
insert overwrite table dws_trade_province_order_nd partition (dt = '2020-06-14')
select province_id,
       province_name,
       area_code,
       iso_code,
       iso_3166_2,
       sum(if(dt >= date_add('2020-06-14', -6), order_count_1d, 0)),
       sum(if(dt >= date_add('2020-06-14', -6), order_original_amount_1d, 0)),
       sum(if(dt >= date_add('2020-06-14', -6), activity_reduce_amount_1d, 0)),
       sum(if(dt >= date_add('2020-06-14', -6), coupon_reduce_amount_1d, 0)),
       sum(if(dt >= date_add('2020-06-14', -6), order_total_amount_1d, 0)),
       sum(order_count_1d),
       sum(order_original_amount_1d),
       sum(activity_reduce_amount_1d),
       sum(coupon_reduce_amount_1d),
       sum(order_total_amount_1d)
from dws_trade_province_order_1d
where dt >= date_add('2020-06-14', -29)
  and dt <= '2020-06-14'
group by province_id, province_name, area_code, iso_code, iso_3166_2;

-- 交易域用户粒度退单最近1日汇总表
CREATE EXTERNAL TABLE dws_trade_user_order_refund_1d
(
    `user_id`                STRING COMMENT '用户id',
    `order_refund_count_1d`  BIGINT COMMENT '最近1日退单次数',
    `order_refund_num_1d`    BIGINT COMMENT '最近1日退单商品件数',
    `order_refund_amount_1d` DECIMAL(16, 2) COMMENT '最近1日退单金额'
) COMMENT '交易域用户粒度退单最近1日汇总事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dws/dws_trade_user_order_refund_1d'
    TBLPROPERTIES ('orc.compress' = 'snappy');

-- 首日装载
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dws_trade_user_order_refund_1d partition (dt)
select user_id,
       count(*)           order_refund_count,
       sum(refund_num)    order_refund_num,
       sum(refund_amount) order_refund_amount,
       dt
from dwd_trade_order_refund_inc
group by user_id, dt;

-- 每日装载
insert overwrite table dws_trade_user_order_refund_1d partition (dt = '2020-06-15')
select user_id,
       count(*),
       sum(refund_num),
       sum(refund_amount)
from dwd_trade_order_refund_inc
where dt = '2020-06-15'
group by user_id;

-- 交易域用户粒度退单最近n日汇总表
CREATE EXTERNAL TABLE dws_trade_user_order_refund_nd
(
    `user_id`                 STRING COMMENT '用户id',
    `order_refund_count_7d`   BIGINT COMMENT '最近7日退单次数',
    `order_refund_num_7d`     BIGINT COMMENT '最近7日退单商品件数',
    `order_refund_amount_7d`  DECIMAL(16, 2) COMMENT '最近7日退单金额',
    `order_refund_count_30d`  BIGINT COMMENT '最近30日退单次数',
    `order_refund_num_30d`    BIGINT COMMENT '最近30日退单商品件数',
    `order_refund_amount_30d` DECIMAL(16, 2) COMMENT '最近30日退单金额'
) COMMENT '交易域用户粒度退单最近n日汇总事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dws/dws_trade_user_order_refund_nd'
    TBLPROPERTIES ('orc.compress' = 'snappy');

-- 数据装载
insert overwrite table dws_trade_user_order_refund_nd partition (dt = '2020-06-14')
select user_id,
       sum(if(dt >= date_add('2020-06-14', -6), order_refund_count_1d, 0)),
       sum(if(dt >= date_add('2020-06-14', -6), order_refund_num_1d, 0)),
       sum(if(dt >= date_add('2020-06-14', -6), order_refund_amount_1d, 0)),
       sum(order_refund_count_1d),
       sum(order_refund_num_1d),
       sum(order_refund_amount_1d)
from dws_trade_user_order_refund_1d
where dt >= date_add('2020-06-14', -29)
  and dt <= '2020-06-14'
group by user_id;

-- 流量域会话粒度页面浏览最近1日汇总表
CREATE EXTERNAL TABLE dws_traffic_session_page_view_1d
(
    `session_id`     STRING COMMENT '会话id',
    `mid_id`         string comment '设备id',
    `brand`          string comment '手机品牌',
    `model`          string comment '手机型号',
    `operate_system` string comment '操作系统',
    `version_code`   string comment 'app版本号',
    `channel`        string comment '渠道',
    `during_time_1d` BIGINT COMMENT '最近1日访问时长',
    `page_count_1d`  BIGINT COMMENT '最近1日访问页面数'
) COMMENT '流量域会话粒度页面浏览最近1日汇总表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dws/dws_traffic_session_page_view_1d'
    TBLPROPERTIES ('orc.compress' = 'snappy');

-- 数据装载
insert overwrite table dws_traffic_session_page_view_1d partition (dt = '2020-06-14')
select session_id,
       mid_id,
       brand,
       model,
       operate_system,
       version_code,
       channel,
       sum(during_time),
       count(*)
from dwd_traffic_page_view_inc
where dt = '2020-06-14'
group by session_id, mid_id, brand, model, operate_system, version_code, channel;

-- 流量域访客页面粒度页面浏览最近1日汇总表
CREATE EXTERNAL TABLE dws_traffic_page_visitor_page_view_1d
(
    `mid_id`         STRING COMMENT '访客id',
    `brand`          string comment '手机品牌',
    `model`          string comment '手机型号',
    `operate_system` string comment '操作系统',
    `page_id`        STRING COMMENT '页面id',
    `during_time_1d` BIGINT COMMENT '最近1日浏览时长',
    `view_count_1d`  BIGINT COMMENT '最近1日访问次数'
) COMMENT '流量域访客页面粒度页面浏览最近1日汇总事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dws/dws_traffic_page_visitor_page_view_1d'
    TBLPROPERTIES ('orc.compress' = 'snappy');

-- 数据装载
insert overwrite table dws_traffic_page_visitor_page_view_1d partition (dt = '2020-06-14')
select mid_id,
       brand,
       model,
       operate_system,
       page_id,
       sum(during_time),
       count(*)
from dwd_traffic_page_view_inc
where dt = '2020-06-14'
group by mid_id, brand, model, operate_system, page_id;

-- 流量域访客页面粒度页面浏览最近n日汇总表
CREATE EXTERNAL TABLE dws_traffic_page_visitor_page_view_nd
(
    `mid_id`          STRING COMMENT '访客id',
    `brand`           string comment '手机品牌',
    `model`           string comment '手机型号',
    `operate_system`  string comment '操作系统',
    `page_id`         STRING COMMENT '页面id',
    `during_time_7d`  BIGINT COMMENT '最近7日浏览时长',
    `view_count_7d`   BIGINT COMMENT '最近7日访问次数',
    `during_time_30d` BIGINT COMMENT '最近30日浏览时长',
    `view_count_30d`  BIGINT COMMENT '最近30日访问次数'
) COMMENT '流量域访客页面粒度页面浏览最近n日汇总事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dws/dws_traffic_page_visitor_page_view_nd'
    TBLPROPERTIES ('orc.compress' = 'snappy');

-- 数据装载
insert overwrite table dws_traffic_page_visitor_page_view_nd partition (dt = '2020-06-14')
select mid_id,
       brand,
       model,
       operate_system,
       page_id,
       sum(if(dt >= date_add('2020-06-14', -6), during_time_1d, 0)),
       sum(if(dt >= date_add('2020-06-14', -6), view_count_1d, 0)),
       sum(during_time_1d),
       sum(view_count_1d)
from dws_traffic_page_visitor_page_view_1d
where dt >= date_add('2020-06-14', -29)
  and dt <= '2020-06-14'
group by mid_id, brand, model, operate_system, page_id;

-- 交易域优惠券粒度订单最近n日汇总表
CREATE EXTERNAL TABLE dws_trade_coupon_order_nd
(
    `coupon_id`                STRING COMMENT '优惠券id',
    `coupon_name`              STRING COMMENT '优惠券名称',
    `coupon_type_code`         STRING COMMENT '优惠券类型id',
    `coupon_type_name`         STRING COMMENT '优惠券类型名称',
    `coupon_rule`              STRING COMMENT '优惠券规则',
    `start_date`               STRING COMMENT '发布日期',
    `original_amount_30d`      DECIMAL(16, 2) COMMENT '使用下单原始金额',
    `coupon_reduce_amount_30d` DECIMAL(16, 2) COMMENT '使用下单优惠金额'
) COMMENT '交易域优惠券粒度订单最近n日汇总事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dws/dws_trade_coupon_order_nd'
    TBLPROPERTIES ('orc.compress' = 'snappy');

-- 数据装载
insert overwrite table dws_trade_coupon_order_nd partition (dt = '2020-06-14')
select id,
       coupon_name,
       coupon_type_code,
       coupon_type_name,
       benefit_rule,
       start_date,
       sum(split_original_amount),
       sum(split_coupon_amount)
from (
         select id,
                coupon_name,
                coupon_type_code,
                coupon_type_name,
                benefit_rule,
                date_format(start_time, 'yyyy-MM-dd') start_date
         from dim_coupon_full
         where dt = '2020-06-14'
           and date_format(start_time, 'yyyy-MM-dd') >= date_add('2020-06-14', -29)
     ) cou
         left join
     (
         select coupon_id,
                order_id,
                split_original_amount,
                split_coupon_amount
         from dwd_trade_order_detail_inc
         where dt >= date_add('2020-06-14', -29)
           and dt <= '2020-06-14'
           and coupon_id is not null
     ) od
     on cou.id = od.coupon_id
group by id, coupon_name, coupon_type_code, coupon_type_name, benefit_rule, start_date;

-- 交易域活动粒度订单最近n日汇总表
CREATE EXTERNAL TABLE dws_trade_activity_order_nd
(
    `activity_id`                STRING COMMENT '活动id',
    `activity_name`              STRING COMMENT '活动名称',
    `activity_type_code`         STRING COMMENT '活动类型编码',
    `activity_type_name`         STRING COMMENT '活动类型名称',
    `start_date`                 STRING COMMENT '发布日期',
    `original_amount_30d`        DECIMAL(16, 2) COMMENT '参与活动订单原始金额',
    `activity_reduce_amount_30d` DECIMAL(16, 2) COMMENT '参与活动订单优惠金额'
) COMMENT '交易域活动粒度订单最近n日汇总事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dws/dws_trade_activity_order_nd'
    TBLPROPERTIES ('orc.compress' = 'snappy');

-- 数据装载
insert overwrite table dws_trade_activity_order_nd partition (dt = '2020-06-14')
select act.activity_id,
       activity_name,
       activity_type_code,
       activity_type_name,
       date_format(start_time, 'yyyy-MM-dd'),
       sum(split_original_amount),
       sum(split_activity_amount)
from (
         select activity_id,
                activity_name,
                activity_type_code,
                activity_type_name,
                start_time
         from dim_activity_full
         where dt = '2020-06-14'
           and date_format(start_time, 'yyyy-MM-dd') >= date_add('2020-06-14', -29)
         group by activity_id, activity_name, activity_type_code, activity_type_name, start_time
     ) act
         left join
     (
         select activity_id,
                order_id,
                split_original_amount,
                split_activity_amount
         from dwd_trade_order_detail_inc
         where dt >= date_add('2020-06-14', -29)
           and dt <= '2020-06-14'
           and activity_id is not null
     ) od
     on act.activity_id = od.activity_id
group by act.activity_id, activity_name, activity_type_code, activity_type_name, start_time;

-- 用户域用户粒度登录历史至今汇总表
CREATE EXTERNAL TABLE dws_user_user_login_td
(
    `user_id`         STRING COMMENT '用户id',
    `login_date_last` STRING COMMENT '末次登录日期',
    `login_count_td`  BIGINT COMMENT '累计登录次数'
) COMMENT '用户域用户粒度登录历史至今汇总事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dws/dws_user_user_login_td'
    TBLPROPERTIES ('orc.compress' = 'snappy');

-- 首日装载
insert overwrite table dws_user_user_login_td partition (dt = '2020-06-14')
select u.id,
       nvl(login_date_last, date_format(create_time, 'yyyy-MM-dd')),
       nvl(login_count_td, 1)
from (
         select id,
                create_time
         from dim_user_zip
         where dt = '9999-12-31'
     ) u
         left join
     (
         select user_id,
                max(dt)  login_date_last,
                count(*) login_count_td
         from dwd_user_login_inc
         group by user_id
     ) l
     on u.id = l.user_id;

-- 每日装载
insert overwrite table dws_user_user_login_td partition (dt = '2020-06-15')
select nvl(old.user_id, new.user_id),
       if(new.user_id is null, old.login_date_last, '2020-06-15'),
       nvl(old.login_count_td, 0) + nvl(new.login_count_1d, 0)
from (
         select user_id,
                login_date_last,
                login_count_td
         from dws_user_user_login_td
         where dt = date_add('2020-06-15', -1)
     ) old
         full outer join
     (
         select user_id,
                count(*) login_count_1d
         from dwd_user_login_inc
         where dt = '2020-06-15'
         group by user_id
     ) new
     on old.user_id = new.user_id;

/*
ADS 层：统计需求
*/

-- 各渠道流量统计
CREATE EXTERNAL TABLE ads_traffic_stats_by_channel
(
    `dt`               STRING COMMENT '统计日期',
    `recent_days`      BIGINT COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    `channel`          STRING COMMENT '渠道',
    `uv_count`         BIGINT COMMENT '访客人数',
    `avg_duration_sec` BIGINT COMMENT '会话平均停留时长，单位为秒',
    `avg_page_count`   BIGINT COMMENT '会话平均浏览页面数',
    `sv_count`         BIGINT COMMENT '会话数',
    `bounce_rate`      DECIMAL(16, 2) COMMENT '跳出率'
) COMMENT '各渠道流量统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/gmall/ads/ads_traffic_stats_by_channel/';

-- 数据装载，方案一
select '2020-06-14',
       1,
       channel,
       count(distinct (mid_id)),
       avg(during_time_1d) / 1000,
       avg(page_count_1d),
       count(*),
       sum(if(page_count_1d = 1, 1, 0)) / count(*)
from dws_traffic_session_page_view_1d
where dt = '2020-06-14'
group by channel
union all
select '2020-06-14',
       7,
       channel,
       count(distinct (mid_id)),
       avg(during_time_1d) / 1000,
       avg(page_count_1d),
       count(*),
       sum(if(page_count_1d = 1, 1, 0)) / count(*)
from dws_traffic_session_page_view_1d
where dt >= date_sub('2020-06-14', 6)
group by channel
union all
select '2020-06-14',
       30,
       channel,
       count(distinct (mid_id)),
       avg(during_time_1d) / 1000,
       avg(page_count_1d),
       count(*),
       sum(if(page_count_1d = 1, 1, 0)) / count(*)
from dws_traffic_session_page_view_1d
where dt >= date_sub('2020-06-14', 29)
group by channel;

-- 数据装载，方案二
insert overwrite table ads_traffic_stats_by_channel
select *
from ads_traffic_stats_by_channel
-- 数据类型和数据内容一致才会去重
union
select '2020-06-14'                                                        dt,
       recent_days,
       channel,
       cast(count(distinct (mid_id)) as bigint)                            uv_count,
       cast(avg(during_time_1d) / 1000 as bigint)                          avg_duration_sec,
       cast(avg(page_count_1d) as bigint)                                  avg_page_count,
       cast(count(*) as bigint)                                            sv_count,
       cast(sum(if(page_count_1d = 1, 1, 0)) / count(*) as decimal(16, 2)) bounce_rate
from dws_traffic_session_page_view_1d lateral view explode(array(1, 7, 30)) tmp as recent_days
where dt >= date_add('2020-06-14', -recent_days + 1)
group by recent_days, channel;

-- 路径分析
CREATE EXTERNAL TABLE ads_page_path
(
    `dt`          STRING COMMENT '统计日期',
    `recent_days` BIGINT COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    `source`      STRING COMMENT '跳转起始页面ID',
    `target`      STRING COMMENT '跳转终到页面ID',
    `path_count`  BIGINT COMMENT '跳转次数'
) COMMENT '页面浏览路径分析'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/gmall/ads/ads_page_path/';

-- 数据装载
insert overwrite table ads_page_path
select *
from ads_page_path
union
select '2020-06-14' dt,
       recent_days,
       source,
       nvl(target, 'null'),
       count(*)     path_count
from (
         select recent_days,
                concat('step-', rn, ':', page_id)          source,
                concat('step-', rn + 1, ':', next_page_id) target
         from (
                  select recent_days,
                         page_id,
--                       lead() 用于提取当前行,后某行的数据，提取page_id字段下1行同字段的数据，没有匹配的为null
                         lead(page_id, 1, null) over (partition by session_id,recent_days)          next_page_id,
                         row_number() over (partition by session_id,recent_days order by view_time) rn
                  from dwd_traffic_page_view_inc lateral view explode(array(1, 7, 30)) tmp as recent_days
                  where dt >= date_add('2020-06-14', -recent_days + 1)
              ) t1
     ) t2
group by recent_days, source, target;

-- 用户变动统计
CREATE EXTERNAL TABLE ads_user_change
(
    `dt`               STRING COMMENT '统计日期',
    `user_churn_count` BIGINT COMMENT '流失用户数',
    `user_back_count`  BIGINT COMMENT '回流用户数'
) COMMENT '用户变动统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/gmall/ads/ads_user_change/';

-- 数据装载
insert overwrite table ads_user_change
select *
from ads_user_change
union
select churn.dt,
       user_churn_count,
       user_back_count
from (
         select '2020-06-14' dt,
                count(*)     user_churn_count
         from dws_user_user_login_td
         where dt = '2020-06-14'
           and login_date_last = date_add('2020-06-14', -7)
     ) churn
         join
     (
         select '2020-06-14' dt,
                count(*)     user_back_count
         from (
                  select user_id,
                         login_date_last
                  from dws_user_user_login_td
                  where dt = '2020-06-14'
              ) t1
                  join
              (
                  select user_id,
                         login_date_last login_date_previous
                  from dws_user_user_login_td
                  where dt = date_add('2020-06-14', -1)
              ) t2
              on t1.user_id = t2.user_id
--       datediff() : 计算两个时间的差值
         where datediff(login_date_last, login_date_previous) >= 8
     ) back
     on churn.dt = back.dt;

-- 用户留存率
CREATE EXTERNAL TABLE ads_user_retention
(
    `dt`              STRING COMMENT '统计日期',
    `create_date`     STRING COMMENT '用户新增日期',
    `retention_day`   INT COMMENT '截至当前日期留存天数',
    `retention_count` BIGINT COMMENT '留存用户数量',
    `new_user_count`  BIGINT COMMENT '新增用户数量',
    `retention_rate`  DECIMAL(16, 2) COMMENT '留存率'
) COMMENT '用户留存率'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/gmall/ads/ads_user_retention/';

-- 数据装载
insert overwrite table ads_user_retention
select *
from ads_user_retention
union
select '2020-06-14'                                                                           dt,
       login_date_first                                                                       create_date,
       datediff('2020-06-14', login_date_first)                                               retention_day,
       sum(if(login_date_last = '2020-06-14', 1, 0))                                          retention_count,
       count(*)                                                                               new_user_count,
       cast(sum(if(login_date_last = '2020-06-14', 1, 0)) / count(*) * 100 as decimal(16, 2)) retention_rate
from (
         select user_id,
                date_id login_date_first
         from dwd_user_register_inc
         where dt >= date_add('2020-06-14', -7)
           and dt < '2020-06-14'
     ) t1
         join
     (
         select user_id,
                login_date_last
         from dws_user_user_login_td
         where dt = '2020-06-14'
     ) t2
     on t1.user_id = t2.user_id
group by login_date_first;

-- 用户新增活跃统计
CREATE EXTERNAL TABLE ads_user_stats
(
    `dt`                STRING COMMENT '统计日期',
    `recent_days`       BIGINT COMMENT '最近n日,1:最近1日,7:最近7日,30:最近30日',
    `new_user_count`    BIGINT COMMENT '新增用户数',
    `active_user_count` BIGINT COMMENT '活跃用户数'
) COMMENT '用户新增活跃统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/gmall/ads/ads_user_stats/';

-- 数据装载
insert overwrite table ads_user_stats
select *
from ads_user_stats
union
select '2020-06-14' dt,
       t1.recent_days,
       new_user_count,
       active_user_count
from (
         select recent_days,
                sum(if(login_date_last >= date_add('2020-06-14', -recent_days + 1), 1, 0)) new_user_count
         from dws_user_user_login_td lateral view explode(array(1, 7, 30)) tmp as recent_days
         where dt = '2020-06-14'
         group by recent_days
     ) t1
         join
     (
         select recent_days,
                sum(if(date_id >= date_add('2020-06-14', -recent_days + 1), 1, 0)) active_user_count
         from dwd_user_register_inc lateral view explode(array(1, 7, 30)) tmp as recent_days
         group by recent_days
     ) t2
     on t1.recent_days = t2.recent_days;

-- 用户行为漏斗分析
CREATE EXTERNAL TABLE ads_user_action
(
    `dt`                STRING COMMENT '统计日期',
    `recent_days`       BIGINT COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    `home_count`        BIGINT COMMENT '浏览首页人数',
    `good_detail_count` BIGINT COMMENT '浏览商品详情页人数',
    `cart_count`        BIGINT COMMENT '加入购物车人数',
    `order_count`       BIGINT COMMENT '下单人数',
    `payment_count`     BIGINT COMMENT '支付人数'
) COMMENT '漏斗分析'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/gmall/ads/ads_user_action/';

-- 数据装载
insert overwrite table ads_user_action
select *
from ads_user_action
union
select '2020-06-14' dt,
       page.recent_days,
       home_count,
       good_detail_count,
       cart_count,
       order_count,
       payment_count
from (
         select 1                                      recent_days,
                sum(if(page_id = 'home', 1, 0))        home_count,
                sum(if(page_id = 'good_detail', 1, 0)) good_detail_count
         from dws_traffic_page_visitor_page_view_1d
         where dt = '2020-06-14'
           and page_id in ('home', 'good_detail')
         union all
         select recent_days,
                sum(if(page_id = 'home' and view_count > 0, 1, 0)),
                sum(if(page_id = 'good_detail' and view_count > 0, 1, 0))
         from (
                  select recent_days,
                         page_id,
                         case recent_days
                             when 7 then view_count_7d
                             when 30 then view_count_30d
                             end view_count
                  from dws_traffic_page_visitor_page_view_nd lateral view explode(array(7, 30)) tmp as recent_days
                  where dt = '2020-06-14'
                    and page_id in ('home', 'good_detail')
              ) t1
         group by recent_days
     ) page
         join
     (
         select 1        recent_days,
                count(*) cart_count
         from dws_trade_user_cart_add_1d
         where dt = '2020-06-14'
         union all
         select recent_days,
                sum(if(cart_count > 0, 1, 0))
         from (
                  select recent_days,
                         case recent_days
                             when 7 then cart_add_count_7d
                             when 30 then cart_add_count_30d
                             end cart_count
                  from dws_trade_user_cart_add_nd lateral view explode(array(7, 30)) tmp as recent_days
                  where dt = '2020-06-14'
              ) t1
         group by recent_days
     ) cart
     on page.recent_days = cart.recent_days
         join
     (
         select 1        recent_days,
                count(*) order_count
         from dws_trade_user_order_1d
         where dt = '2020-06-14'
         union all
         select recent_days,
                sum(if(order_count > 0, 1, 0))
         from (
                  select recent_days,
                         case recent_days
                             when 7 then order_count_7d
                             when 30 then order_count_30d
                             end order_count
                  from dws_trade_user_order_nd lateral view explode(array(7, 30)) tmp as recent_days
                  where dt = '2020-06-14'
              ) t1
         group by recent_days
     ) ord
     on page.recent_days = ord.recent_days
         join
     (
         select 1        recent_days,
                count(*) payment_count
         from dws_trade_user_payment_1d
         where dt = '2020-06-14'
         union all
         select recent_days,
                sum(if(order_count > 0, 1, 0))
         from (
                  select recent_days,
                         case recent_days
                             when 7 then payment_count_7d
                             when 30 then payment_count_30d
                             end order_count
                  from dws_trade_user_payment_nd lateral view explode(array(7, 30)) tmp as recent_days
                  where dt = '2020-06-14'
              ) t1
         group by recent_days
     ) pay
     on page.recent_days = pay.recent_days;

-- 新增交易用户统计
CREATE EXTERNAL TABLE ads_new_buyer_stats
(
    `dt`                     STRING COMMENT '统计日期',
    `recent_days`            BIGINT COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    `new_order_user_count`   BIGINT COMMENT '新增下单人数',
    `new_payment_user_count` BIGINT COMMENT '新增支付人数'
) COMMENT '新增交易用户统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/gmall/ads/ads_new_buyer_stats/';

-- 数据装载
insert overwrite table ads_new_buyer_stats
select *
from ads_new_buyer_stats
union
select '2020-06-14',
       odr.recent_days,
       new_order_user_count,
       new_payment_user_count
from (
         select recent_days,
                sum(if(order_date_first >= date_add('2020-06-14', -recent_days + 1), 1, 0)) new_order_user_count
         from dws_trade_user_order_td lateral view explode(array(1, 7, 30)) tmp as recent_days
         where dt = '2020-06-14'
         group by recent_days
     ) odr
         join
     (
         select recent_days,
                sum(if(payment_date_first >= date_add('2020-06-14', -recent_days + 1), 1, 0)) new_payment_user_count
         from dws_trade_user_payment_td lateral view explode(array(1, 7, 30)) tmp as recent_days
         where dt = '2020-06-14'
         group by recent_days
     ) pay
     on odr.recent_days = pay.recent_days;

-- 最近7/30日各品牌复购率
CREATE EXTERNAL TABLE ads_repeat_purchase_by_tm
(
    `dt`                STRING COMMENT '统计日期',
    `recent_days`       BIGINT COMMENT '最近天数,7:最近7天,30:最近30天',
    `tm_id`             STRING COMMENT '品牌ID',
    `tm_name`           STRING COMMENT '品牌名称',
    `order_repeat_rate` DECIMAL(16, 2) COMMENT '复购率'
) COMMENT '各品牌复购率统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/gmall/ads/ads_repeat_purchase_by_tm/';

-- 数据装载
insert overwrite table ads_repeat_purchase_by_tm
select *
from ads_repeat_purchase_by_tm
union
select '2020-06-14' dt,
       recent_days,
       tm_id,
       tm_name,
       cast(sum(if(order_count >= 2, 1, 0)) / sum(if(order_count >= 1, 1, 0)) as decimal(16, 2))
from (
         select '2020-06-14'     dt,
                recent_days,
                user_id,
                tm_id,
                tm_name,
                sum(order_count) order_count
         from (
                  select recent_days,
                         user_id,
                         tm_id,
                         tm_name,
                         case recent_days
                             when 7 then order_count_7d
                             when 30 then order_count_30d
                             end order_count
                  from dws_trade_user_sku_order_nd lateral view explode(array(7, 30)) tmp as recent_days
                  where dt = '2020-06-14'
              ) t1
         group by recent_days, user_id, tm_id, tm_name
     ) t2
group by recent_days, tm_id, tm_name;

-- 各品牌商品交易统计
CREATE EXTERNAL TABLE ads_trade_stats_by_tm
(
    `dt`                      STRING COMMENT '统计日期',
    `recent_days`             BIGINT COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    `tm_id`                   STRING COMMENT '品牌ID',
    `tm_name`                 STRING COMMENT '品牌名称',
    `order_count`             BIGINT COMMENT '订单数',
    `order_user_count`        BIGINT COMMENT '订单人数',
    `order_refund_count`      BIGINT COMMENT '退单数',
    `order_refund_user_count` BIGINT COMMENT '退单人数'
) COMMENT '各品牌商品交易统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/gmall/ads/ads_trade_stats_by_tm/';

-- 数据装载
insert overwrite table ads_trade_stats_by_tm
select *
from ads_trade_stats_by_tm
union
select '2020-06-14' dt,
       nvl(odr.recent_days, refund.recent_days),
       nvl(odr.tm_id, refund.tm_id),
       nvl(odr.tm_name, refund.tm_name),
       nvl(order_count, 0),
       nvl(order_user_count, 0),
       nvl(order_refund_count, 0),
       nvl(order_refund_user_count, 0)
from (
         select 1                         recent_days,
                tm_id,
                tm_name,
                sum(order_count_1d)       order_count,
                count(distinct (user_id)) order_user_count
         from dws_trade_user_sku_order_1d
         where dt = '2020-06-14'
         group by tm_id, tm_name
         union all
         select recent_days,
                tm_id,
                tm_name,
                sum(order_count),
                count(distinct (if(order_count > 0, user_id, null)))
         from (
                  select recent_days,
                         user_id,
                         tm_id,
                         tm_name,
                         case recent_days
                             when 7 then order_count_7d
                             when 30 then order_count_30d
                             end order_count
                  from dws_trade_user_sku_order_nd lateral view explode(array(7, 30)) tmp as recent_days
                  where dt = '2020-06-14'
              ) t1
         group by recent_days, tm_id, tm_name
     ) odr
         full outer join
     (
         select 1                          recent_days,
                tm_id,
                tm_name,
                sum(order_refund_count_1d) order_refund_count,
                count(distinct (user_id))  order_refund_user_count
         from dws_trade_user_sku_order_refund_1d
         where dt = '2020-06-14'
         group by tm_id, tm_name
         union all
         select recent_days,
                tm_id,
                tm_name,
                sum(order_refund_count),
                count(if(order_refund_count > 0, user_id, null))
         from (
                  select recent_days,
                         user_id,
                         tm_id,
                         tm_name,
                         case recent_days
                             when 7 then order_refund_count_7d
                             when 30 then order_refund_count_30d
                             end order_refund_count
                  from dws_trade_user_sku_order_refund_nd lateral view explode(array(7, 30)) tmp as recent_days
                  where dt = '2020-06-14'
              ) t1
         group by recent_days, tm_id, tm_name
     ) refund
     on odr.recent_days = refund.recent_days
         and odr.tm_id = refund.tm_id
         and odr.tm_name = refund.tm_name;

-- 各品类商品交易统计
CREATE EXTERNAL TABLE ads_trade_stats_by_cate
(
    `dt`                      STRING COMMENT '统计日期',
    `recent_days`             BIGINT COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    `category1_id`            STRING COMMENT '一级分类id',
    `category1_name`          STRING COMMENT '一级分类名称',
    `category2_id`            STRING COMMENT '二级分类id',
    `category2_name`          STRING COMMENT '二级分类名称',
    `category3_id`            STRING COMMENT '三级分类id',
    `category3_name`          STRING COMMENT '三级分类名称',
    `order_count`             BIGINT COMMENT '订单数',
    `order_user_count`        BIGINT COMMENT '订单人数',
    `order_refund_count`      BIGINT COMMENT '退单数',
    `order_refund_user_count` BIGINT COMMENT '退单人数'
) COMMENT '各分类商品交易统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/gmall/ads/ads_trade_stats_by_cate/';

-- 数据装载
insert overwrite table ads_trade_stats_by_cate
select *
from ads_trade_stats_by_cate
union
select '2020-06-14' dt,
       nvl(odr.recent_days, refund.recent_days),
       nvl(odr.category1_id, refund.category1_id),
       nvl(odr.category1_name, refund.category1_name),
       nvl(odr.category2_id, refund.category2_id),
       nvl(odr.category2_name, refund.category2_name),
       nvl(odr.category3_id, refund.category3_id),
       nvl(odr.category3_name, refund.category3_name),
       nvl(order_count, 0),
       nvl(order_user_count, 0),
       nvl(order_refund_count, 0),
       nvl(order_refund_user_count, 0)
from (
         select 1                         recent_days,
                category1_id,
                category1_name,
                category2_id,
                category2_name,
                category3_id,
                category3_name,
                sum(order_count_1d)       order_count,
                count(distinct (user_id)) order_user_count
         from dws_trade_user_sku_order_1d
         where dt = '2020-06-14'
         group by category1_id, category1_name, category2_id, category2_name, category3_id, category3_name
         union all
         select recent_days,
                category1_id,
                category1_name,
                category2_id,
                category2_name,
                category3_id,
                category3_name,
                sum(order_count),
                count(distinct (if(order_count > 0, user_id, null)))
         from (
                  select recent_days,
                         user_id,
                         category1_id,
                         category1_name,
                         category2_id,
                         category2_name,
                         category3_id,
                         category3_name,
                         case recent_days
                             when 7 then order_count_7d
                             when 30 then order_count_30d
                             end order_count
                  from dws_trade_user_sku_order_nd lateral view explode(array(7, 30)) tmp as recent_days
                  where dt = '2020-06-14'
              ) t1
         group by recent_days, category1_id, category1_name, category2_id, category2_name, category3_id, category3_name
     ) odr
         full outer join
     (
         select 1                          recent_days,
                category1_id,
                category1_name,
                category2_id,
                category2_name,
                category3_id,
                category3_name,
                sum(order_refund_count_1d) order_refund_count,
                count(distinct (user_id))  order_refund_user_count
         from dws_trade_user_sku_order_refund_1d
         where dt = '2020-06-14'
         group by category1_id, category1_name, category2_id, category2_name, category3_id, category3_name
         union all
         select recent_days,
                category1_id,
                category1_name,
                category2_id,
                category2_name,
                category3_id,
                category3_name,
                sum(order_refund_count),
                count(distinct (if(order_refund_count > 0, user_id, null)))
         from (
                  select recent_days,
                         user_id,
                         category1_id,
                         category1_name,
                         category2_id,
                         category2_name,
                         category3_id,
                         category3_name,
                         case recent_days
                             when 7 then order_refund_count_7d
                             when 30 then order_refund_count_30d
                             end order_refund_count
                  from dws_trade_user_sku_order_refund_nd lateral view explode(array(7, 30)) tmp as recent_days
                  where dt = '2020-06-14'
              ) t1
         group by recent_days, category1_id, category1_name, category2_id, category2_name, category3_id, category3_name
     ) refund
     on odr.recent_days = refund.recent_days
         and odr.category1_id = refund.category1_id
         and odr.category1_name = refund.category1_name
         and odr.category2_id = refund.category2_id
         and odr.category2_name = refund.category2_name
         and odr.category3_id = refund.category3_id
         and odr.category3_name = refund.category3_name;

-- 各分类商品购物车存量Top3
CREATE EXTERNAL TABLE ads_sku_cart_num_top3_by_cate
(
    `dt`             STRING COMMENT '统计日期',
    `category1_id`   STRING COMMENT '一级分类ID',
    `category1_name` STRING COMMENT '一级分类名称',
    `category2_id`   STRING COMMENT '二级分类ID',
    `category2_name` STRING COMMENT '二级分类名称',
    `category3_id`   STRING COMMENT '三级分类ID',
    `category3_name` STRING COMMENT '三级分类名称',
    `sku_id`         STRING COMMENT '商品id',
    `sku_name`       STRING COMMENT '商品名称',
    `cart_num`       BIGINT COMMENT '购物车中商品数量',
    `rk`             BIGINT COMMENT '排名'
) COMMENT '各分类商品购物车存量Top10'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/gmall/ads/ads_sku_cart_num_top3_by_cate/';

-- 数据装载
insert overwrite table ads_sku_cart_num_top3_by_cate
select *
from ads_sku_cart_num_top3_by_cate
union
select '2020-06-14' dt,
       category1_id,
       category1_name,
       category2_id,
       category2_name,
       category3_id,
       category3_name,
       sku_id,
       sku_name,
       cart_num,
       rk
from (
         select sku_id,
                sku_name,
                category1_id,
                category1_name,
                category2_id,
                category2_name,
                category3_id,
                category3_name,
                cart_num,
                rank() over (partition by category1_id,category2_id,category3_id order by cart_num desc) rk
         from (
                  select sku_id,
                         sum(sku_num) cart_num
                  from dwd_trade_cart_full
                  where dt = '2020-06-14'
                  group by sku_id
              ) cart
                  left join
              (
                  select id,
                         sku_name,
                         category1_id,
                         category1_name,
                         category2_id,
                         category2_name,
                         category3_id,
                         category3_name
                  from dim_sku_full
                  where dt = '2020-06-14'
              ) sku
              on cart.sku_id = sku.id
     ) t1
where rk <= 3;

-- 交易综合统计
CREATE EXTERNAL TABLE ads_trade_stats
(
    `dt`                      STRING COMMENT '统计日期',
    `recent_days`             BIGINT COMMENT '最近天数,1:最近1日,7:最近7天,30:最近30天',
    `order_total_amount`      DECIMAL(16, 2) COMMENT '订单总额,GMV',
    `order_count`             BIGINT COMMENT '订单数',
    `order_user_count`        BIGINT COMMENT '下单人数',
    `order_refund_count`      BIGINT COMMENT '退单数',
    `order_refund_user_count` BIGINT COMMENT '退单人数'
) COMMENT '交易统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/gmall/ads/ads_trade_stats/';

-- 数据装载
insert overwrite table ads_trade_stats
select *
from ads_trade_stats
union
select '2020-06-14',
       odr.recent_days,
       order_total_amount,
       order_count,
       order_user_count,
       order_refund_count,
       order_refund_user_count
from (
         select 1                          recent_days,
                sum(order_total_amount_1d) order_total_amount,
                sum(order_count_1d)        order_count,
                count(*)                   order_user_count
         from dws_trade_user_order_1d
         where dt = '2020-06-14'
         union all
         select recent_days,
                sum(order_total_amount),
                sum(order_count),
                sum(if(order_count > 0, 1, 0))
         from (
                  select recent_days,
                         case recent_days
                             when 7 then order_total_amount_7d
                             when 30 then order_total_amount_30d
                             end order_total_amount,
                         case recent_days
                             when 7 then order_count_7d
                             when 30 then order_count_30d
                             end order_count
                  from dws_trade_user_order_nd lateral view explode(array(7, 30)) tmp as recent_days
                  where dt = '2020-06-14'
              ) t1
         group by recent_days
     ) odr
         join
     (
         select 1                          recent_days,
                sum(order_refund_count_1d) order_refund_count,
                count(*)                   order_refund_user_count
         from dws_trade_user_order_refund_1d
         where dt = '2020-06-14'
         union all
         select recent_days,
                sum(order_refund_count),
                sum(if(order_refund_count > 0, 1, 0))
         from (
                  select recent_days,
                         case recent_days
                             when 7 then order_refund_count_7d
                             when 30 then order_refund_count_30d
                             end order_refund_count
                  from dws_trade_user_order_refund_nd lateral view explode(array(7, 30)) tmp as recent_days
                  where dt = '2020-06-14'
              ) t1
         group by recent_days
     ) refund
     on odr.recent_days = refund.recent_days;

-- 各省份交易统计
CREATE EXTERNAL TABLE ads_order_by_province
(
    `dt`                 STRING COMMENT '统计日期',
    `recent_days`        BIGINT COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    `province_id`        STRING COMMENT '省份ID',
    `province_name`      STRING COMMENT '省份名称',
    `area_code`          STRING COMMENT '地区编码',
    `iso_code`           STRING COMMENT '国际标准地区编码',
    `iso_code_3166_2`    STRING COMMENT '国际标准地区编码',
    `order_count`        BIGINT COMMENT '订单数',
    `order_total_amount` DECIMAL(16, 2) COMMENT '订单金额'
) COMMENT '各地区订单统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/gmall/ads/ads_order_by_province/';

-- 数据装载
insert overwrite table ads_order_by_province
select *
from ads_order_by_province
union
select '2020-06-14' dt,
       1            recent_days,
       province_id,
       province_name,
       area_code,
       iso_code,
       iso_3166_2,
       order_count_1d,
       order_total_amount_1d
from dws_trade_province_order_1d
where dt = '2020-06-14'
union
select '2020-06-14' dt,
       recent_days,
       province_id,
       province_name,
       area_code,
       iso_code,
       iso_3166_2,
       sum(order_count),
       sum(order_total_amount)
from (
         select recent_days,
                province_id,
                province_name,
                area_code,
                iso_code,
                iso_3166_2,
                case recent_days
                    when 7 then order_count_7d
                    when 30 then order_count_30d
                    end order_count,
                case recent_days
                    when 7 then order_total_amount_7d
                    when 30 then order_total_amount_30d
                    end order_total_amount
         from dws_trade_province_order_nd lateral view explode(array(7, 30)) tmp as recent_days
         where dt = '2020-06-14'
     ) t1
group by recent_days, province_id, province_name, area_code, iso_code, iso_3166_2;

-- 最近30天发布的优惠券的补贴率
CREATE EXTERNAL TABLE ads_coupon_stats
(
    `dt`          STRING COMMENT '统计日期',
    `coupon_id`   STRING COMMENT '优惠券ID',
    `coupon_name` STRING COMMENT '优惠券名称',
    `start_date`  STRING COMMENT '发布日期',
    `rule_name`   STRING COMMENT '优惠规则，例如满100元减10元',
    `reduce_rate` DECIMAL(16, 2) COMMENT '补贴率'
) COMMENT '优惠券统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/gmall/ads/ads_coupon_stats/';

-- 数据装载
insert overwrite table ads_coupon_stats
select *
from ads_coupon_stats
union
select '2020-06-14' dt,
       coupon_id,
       coupon_name,
       start_date,
       coupon_rule,
       cast(coupon_reduce_amount_30d / original_amount_30d as decimal(16, 2))
from dws_trade_coupon_order_nd
where dt = '2020-06-14';

-- 最近30天发布的活动的补贴率
CREATE EXTERNAL TABLE ads_activity_stats
(
    `dt`            STRING COMMENT '统计日期',
    `activity_id`   STRING COMMENT '活动ID',
    `activity_name` STRING COMMENT '活动名称',
    `start_date`    STRING COMMENT '活动开始日期',
    `reduce_rate`   DECIMAL(16, 2) COMMENT '补贴率'
) COMMENT '活动统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/gmall/ads/ads_activity_stats/';

-- 数据装载
insert overwrite table ads_activity_stats
select *
from ads_activity_stats
union
select '2020-06-14' dt,
       activity_id,
       activity_name,
       start_date,
       cast(activity_reduce_amount_30d / original_amount_30d as decimal(16, 2))
from dws_trade_activity_order_nd
where dt = '2020-06-14';
