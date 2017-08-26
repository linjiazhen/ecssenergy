package com.wintoo.model;

import lombok.Data;

/**
 * Created by Jason on 16/8/31.
 */
@Data
public class EnergyAlert {
    private String item1;
    private String item2;
    private String item3;
    private String energyitem;
    private double value;
    private double usage;
    private int status;
    private String datetime;
    private String timeunit;

}
