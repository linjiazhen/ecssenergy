package com.wintoo.model;

import lombok.Data;

@Data
public class Gateway {
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
	private String address;
    private String code;
    private String ipmain;
    private String portmain;
    private String ipbackup;
    private String portbackup;
	private String apn;
	private int    delay1;
	private int    delay2;
	private int	   waittime;
	private int    flag;
	private int    heartbeat;
	private int    use;
	private int    offline;
    private double interval_time;
	private String newest_heartbeat_time;
	private String login_time;
	private String lost_connet_time;
	private String gateway_ver;
	private String zd_ip;
	private String zd_mask;
	private String zd_gateway;
	private String zd_mac;
	private String zd_lcd_password;
	private int    server_flag;
    private String status;
}
