package com.wintoo.model;

import lombok.Data;

import java.math.BigDecimal;

/**
 * Created by Jason on 15/10/28.
 */
@Data
public class AmCalData {
    private String equipid;
    private String datetime;
    private BigDecimal value;

}
