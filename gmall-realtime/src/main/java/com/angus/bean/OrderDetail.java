package com.angus.bean;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.math.BigDecimal;

/**
 * @author ：Angus
 * @date ：Created in 2022/5/1 22:48
 * @description：
 */
@AllArgsConstructor
@Getter
@Setter
@NoArgsConstructor
public class OrderDetail {
    private Long id;
    private Long order_id;
    private Long sku_id;
    private BigDecimal order_price;
    private Long sku_num;
    private String sku_name;
    private String create_time;
    private BigDecimal split_total_amount;
    private BigDecimal split_activity_amount;
    private BigDecimal split_coupon_amount;
    private Long create_ts;

    @Override
    public String toString() {
        return "OrderDetail{" +
                "id=" + id +
                ", order_id=" + order_id +
                ", sku_id=" + sku_id +
                ", order_price=" + order_price +
                ", sku_num=" + sku_num +
                ", sku_name='" + sku_name + '\'' +
                ", create_time='" + create_time + '\'' +
                ", split_total_amount=" + split_total_amount +
                ", split_activity_amount=" + split_activity_amount +
                ", split_coupon_amount=" + split_coupon_amount +
                ", create_ts=" + create_ts +
                '}';
    }
}
