package com.angus.bean;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * @author ：Angus
 * @date ：Created in 2022/4/25 22:19
 * @description：
 */
@AllArgsConstructor
@Getter
@Setter
@NoArgsConstructor
public class TableProcess {
    // 动态分流Sink常量
    public static final String SINK_TYPE_HBASE = "hbase";
    public static final String SINK_TYPE_KAFKA = "kafka";
    public static final String SINK_TYPE_CLICKHOUSE = "clickhouse";

    // 来源表
    String sourceTable;
    // 操作类型
    String operateType;
    // 输出类型
    String sinkType;
    // 输出表（主题）
    String sinkTable;
    // 表中字段
    String sinkColumns;
    // 主键字段
    String primaryKey;
    // 建表扩展
    String sinkExtend;

    @Override
    public String toString() {
        return "TableProcess{" +
                "sourceTable='" + sourceTable + '\'' +
                ", operateType='" + operateType + '\'' +
                ", sinkType='" + sinkType + '\'' +
                ", sinkTable='" + sinkTable + '\'' +
                ", sinkColumns='" + sinkColumns + '\'' +
                ", primaryKey='" + primaryKey + '\'' +
                ", sinkExtend='" + sinkExtend + '\'' +
                '}';
    }
}
