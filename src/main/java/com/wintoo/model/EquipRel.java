package com.wintoo.model;

import lombok.Data;

import java.math.BigDecimal;

/**
 * Created by Jason on 15/10/28.
 */
@Data
public class EquipRel {
    private String equipid;
    private String buildid;
    private String upequipid;
    private String energytype;
    private BigDecimal    buildlevel;
    private BigDecimal percent;
}
