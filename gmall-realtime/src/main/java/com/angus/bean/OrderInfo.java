package com.angus.bean;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.math.BigDecimal;

/**
 * @author ：Angus
 * @date ：Created in 2022/5/1 22:47
 * @description：
 */
@AllArgsConstructor
@Getter
@Setter
@NoArgsConstructor
public class OrderInfo {
    private Long id;
    private Long province_id;
    private String order_status;
    private Long user_id;
    private BigDecimal total_amount;
    private BigDecimal activity_reduce_amount;
    private BigDecimal coupon_reduce_amount;
    private BigDecimal original_total_amount;
    private BigDecimal feight_fee;
    private String expire_time;
    private String create_time;
    private String operate_time;
    private String create_date; // 把其他字段处理得到
    private String create_hour;
    private Long create_ts;

    @Override
    public String toString() {
        return "OrderInfo{" +
                "id=" + id +
                ", province_id=" + province_id +
                ", order_status='" + order_status + '\'' +
                ", user_id=" + user_id +
                ", total_amount=" + total_amount +
                ", activity_reduce_amount=" + activity_reduce_amount +
                ", coupon_reduce_amount=" + coupon_reduce_amount +
                ", original_total_amount=" + original_total_amount +
                ", feight_fee=" + feight_fee +
                ", expire_time='" + expire_time + '\'' +
                ", create_time='" + create_time + '\'' +
                ", operate_time='" + operate_time + '\'' +
                ", create_date='" + create_date + '\'' +
                ", create_hour='" + create_hour + '\'' +
                ", create_ts=" + create_ts +
                '}';
    }
}
