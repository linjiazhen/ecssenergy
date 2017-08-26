package com.wintoo.model;

import lombok.Data;

import java.math.BigDecimal;

@Data
public class AmmeterData {
	private String uuid;
    private String nodeid;
    private int    type;
	private BigDecimal active;
	private String datatime;
}
