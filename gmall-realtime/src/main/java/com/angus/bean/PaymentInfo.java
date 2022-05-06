package com.angus.bean;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.math.BigDecimal;

/**
 * @author ：Angus
 * @date ：Created in 2022/5/6 21:51
 * @description：
 */
@AllArgsConstructor
@Getter
@Setter
@NoArgsConstructor
public class PaymentInfo {
    Long id;
    Long order_id;
    Long user_id;
    BigDecimal total_amount;
    String subject;
    String payment_type;
    String create_time;
    String callback_time;
}
