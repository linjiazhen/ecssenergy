package com.wintoo.model;

import lombok.Data;

@Data
public class Ammeter {
	private String uuid;
	private String equip;
	private String batch;
	private String type;
	private String subtype;
	private String model;
	private int    installtype;
	private String group;
	private String build;
	private String floor;
	private String room;
	private String remark;
	private String longitude;
	private String latitude;
	private String gateway;
	private String gatewayaddress;
	private String gatewayid;
	private double newest_data;
	private String newest_data_time;
    private String newest_operate_time;
    private double newest_valid_data;
    private double time_interval;
    private double day_valid_data;
    private String status;
    private int statue;
    private int    flag;
    private String remarkinfo;
}
