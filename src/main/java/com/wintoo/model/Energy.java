package com.wintoo.model;

import lombok.Data;

import java.math.BigDecimal;

/**
 * Created by Jason on 15/10/28.
 */
@Data
public class Energy implements Cloneable{
    private String id;
    private String energytype;
    private String starttime;
    private String endtime;
    private BigDecimal value;
    private BigDecimal buildlevel;

}
