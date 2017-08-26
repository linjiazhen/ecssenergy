package com.wintoo.model;

import lombok.Data;

import java.math.BigDecimal;

@Data
public class WatermeterData {
	private String uuid;
    private String nodeid;
    private BigDecimal type;
	private String datatime;
	private BigDecimal u;
	private BigDecimal consum;
	private BigDecimal flow;
}
