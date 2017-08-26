package com.wintoo.monitor;


import com.wintoo.tools.*;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Jason on 15/8/26.
 */
public class EquipMonitor implements Runnable {
    private String mailArray;
    private String phone;
    private EmailService emailService=new EmailService();
    private Connection con = null;
    private PreparedStatement pre = null;
    private ResultSet result = null;
    private Map<String,Boolean> gatewayMap=new HashMap<>();
    private Map<String,Boolean> ammeterMap=new HashMap<>();
    private Map<String,Boolean> watermeterMap=new HashMap<>();
    public EquipMonitor(Connection con){
        try {
            this.con=con;
            pre = con.prepareStatement("SELECT F_UUID FROM T_BE_GATEWAY");
            result = pre.executeQuery();
            while(result.next()) {
                gatewayMap.put(result.getString(1),false);
            }
            pre.close();
            pre = con.prepareStatement("SELECT F_UUID FROM T_BE_AMMETER");
            result = pre.executeQuery();
            while(result.next()) {
                ammeterMap.put(result.getString(1),false);
            }
            pre.close();
            pre = con.prepareStatement("SELECT F_UUID FROM T_BE_WATERMETER");
            result = pre.executeQuery();
            while(result.next()) {
                watermeterMap.put(result.getString(1),false);
            }
            pre.close();
            pre = con.prepareStatement("select a.F_EQUIPUUID from T_EC_EQUIPALERT_RECORD a where a.F_INSERTTIME=(select max(b.F_INSERTTIME) from T_EC_EQUIPALERT_RECORD b where a.F_EQUIPUUID=b.F_EQUIPUUID) and a.F_STATUS!=0");
            result = pre.executeQuery();
            while(result.next()) {
                if(ammeterMap.containsKey(result.getString(1)))
                    ammeterMap.put(result.getString(1),true);
                else
                if(watermeterMap.containsKey(result.getString(1)))
                    watermeterMap.put(result.getString(1),true);
                else
                    gatewayMap.put(result.getString(1),true);
            }
            pre.close();
        } catch (Exception e) {
            // TODO: handle exception
            System.out.println(e);
        }
    }
    private void getMailPhone(){
        mailArray="";
        phone="";
        try {
            String sql="SELECT F_PHONE,F_EMAIL from T_BS_ROLE a,T_BS_USER b where a.F_UUID=b.F_ROLEID and a.F_NAME='后勤管理员'";
            pre = con.prepareStatement(sql);
            result = pre.executeQuery();
            while(result.next()) {
                if(result.getString(1)!=null)
                    phone+=result.getString(1)+",";
                if(result.getString(2)!=null)
                    mailArray+=result.getString(2)+",";
            }
            pre.close();
        }catch (Exception e){
            e.printStackTrace();
        }

    }

    //    private void transGateway(){
//        Set<String> set=new HashSet<>();
//        String sql="select F_UUID,F_BATCHID,F_ADDRESS,F_CODE,F_IP_MAIN,F_PORT_MAIN,F_IP_BACKUP,F_PORT_BACKUP,F_APN,F_DELAY_1,F_DELAY_2," +
//                "F_WAIT_TIME,F_FLAG,F_HEARTBEAT,F_USE,F_HEARTBEAT_TIME,F_LOGIN_TIME,F_LOST_CONNET_TIME,F_GATEWAY_VER,F_ZD_IP,F_ZD_MASK," +
//                "F_ZD_GATEWAY,F_ZD_MAC,F_ZD_LCD_PASSWORD,F_SERVER_FLAG from T_BE_GATEWAY";
//        String sql1="INSERT INTO T_BE_GATEWAY(F_UUID,F_BATCHID,F_ADDRESS,F_CODE,F_IP_MAIN,F_PORT_MAIN,F_IP_BACKUP,F_PORT_BACKUP,F_APN,F_DELAY_1,F_DELAY_2," +
//                "F_WAIT_TIME,F_FLAG,F_HEARTBEAT,F_USE,F_HEARTBEAT_TIME,F_LOGIN_TIME,F_LOST_CONNET_TIME,F_GATEWAY_VER,F_ZD_IP,F_ZD_MASK," +
//                "F_ZD_GATEWAY,F_ZD_MAC,F_ZD_LCD_PASSWORD,F_SERVER_FLAG) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
//        String sql2="UPDATE T_BE_GATEWAY set F_BATCHID=?,F_ADDRESS=?,F_CODE=?,F_IP_MAIN=?,F_PORT_MAIN=?,F_IP_BACKUP=?,F_PORT_BACKUP=?,F_APN=?,F_DELAY_1=?,F_DELAY_2=?," +
//                "F_WAIT_TIME=?,F_FLAG=?,F_HEARTBEAT=?,F_USE=?,F_HEARTBEAT_TIME=?,F_LOGIN_TIME=?,F_LOST_CONNET_TIME=?,F_GATEWAY_VER=?,F_ZD_IP=?,F_ZD_MASK=?," +
//                "F_ZD_GATEWAY=?,F_ZD_MAC=?,F_ZD_LCD_PASSWORD=?,F_SERVER_FLAG=? WHERE F_UUID=?";
//        PreparedStatement preFrom = null;
//        PreparedStatement preTo = null;
//        PreparedStatement preInsert = null;
//        PreparedStatement preUpdate = null;
//        ResultSet resultFrom = null;
//        ResultSet resultTo = null;
//        try{
//            preTo=conTo.prepareStatement("SELECT f_uuid FROM T_BE_GATEWAY");
//            resultTo=preTo.executeQuery();
//            while (resultTo.next()){
//                set.add(resultTo.getString(1));
//            }
//            preFrom=conFrom.prepareStatement(sql);
//            resultFrom=preFrom.executeQuery();
//            preInsert=conTo.prepareStatement(sql1);
//            preUpdate= conTo.prepareStatement(sql2);
//            while (resultFrom.next()){
//                if(set.contains(resultFrom.getString("F_UUID"))){
//                    preUpdate.setString(1,resultFrom.getString("F_BATCHID"));
//                    preUpdate.setString(2,resultFrom.getString("F_ADDRESS"));
//                    preUpdate.setString(3,resultFrom.getString("F_CODE"));
//                    preUpdate.setString(4,resultFrom.getString("F_IP_MAIN"));
//                    preUpdate.setString(5,resultFrom.getString("F_PORT_MAIN"));
//                    preUpdate.setString(6,resultFrom.getString("F_IP_BACKUP"));
//                    preUpdate.setString(7,resultFrom.getString("F_PORT_BACKUP"));
//                    preUpdate.setString(8,resultFrom.getString("F_APN"));
//                    preUpdate.setInt(9,resultFrom.getInt("F_DELAY_1"));
//                    preUpdate.setInt(10,resultFrom.getInt("F_DELAY_2"));
//                    preUpdate.setInt(11,resultFrom.getInt("F_WAIT_TIME"));
//                    preUpdate.setInt(12,resultFrom.getInt("F_FLAG"));
//                    preUpdate.setInt(13,resultFrom.getInt("F_HEARTBEAT"));
//                    preUpdate.setInt(14,resultFrom.getInt("F_USE"));
//                    preUpdate.setTimestamp(15,resultFrom.getTimestamp("F_HEARTBEAT_TIME"));
//                    preUpdate.setTimestamp(16,resultFrom.getTimestamp("F_LOGIN_TIME"));
//                    preUpdate.setTimestamp(17,resultFrom.getTimestamp("F_LOST_CONNET_TIME"));
//                    preUpdate.setString(18,resultFrom.getString("F_GATEWAY_VER"));
//                    preUpdate.setString(19,resultFrom.getString("F_ZD_IP"));
//                    preUpdate.setString(20,resultFrom.getString("F_ZD_MASK"));
//                    preUpdate.setString(21,resultFrom.getString("F_ZD_GATEWAY"));
//                    preUpdate.setString(22,resultFrom.getString("F_ZD_MAC"));
//                    preUpdate.setString(23,resultFrom.getString("F_ZD_LCD_PASSWORD"));
//                    preUpdate.setInt(24,resultFrom.getInt("F_SERVER_FLAG"));
//                    preUpdate.setString(25,resultFrom.getString("F_UUID"));
//                    preUpdate.addBatch();
//                }
//                else{
//                    preInsert.setString(1,resultFrom.getString("F_UUID"));
//                    preInsert.setString(2,resultFrom.getString("F_BATCHID"));
//                    preInsert.setString(3,resultFrom.getString("F_ADDRESS"));
//                    preInsert.setString(4,resultFrom.getString("F_CODE"));
//                    preInsert.setString(5,resultFrom.getString("F_IP_MAIN"));
//                    preInsert.setString(6,resultFrom.getString("F_PORT_MAIN"));
//                    preInsert.setString(7,resultFrom.getString("F_IP_BACKUP"));
//                    preInsert.setString(8,resultFrom.getString("F_PORT_BACKUP"));
//                    preInsert.setString(9,resultFrom.getString("F_APN"));
//                    preInsert.setInt(10,resultFrom.getInt("F_DELAY_1"));
//                    preInsert.setInt(11,resultFrom.getInt("F_DELAY_2"));
//                    preInsert.setInt(12,resultFrom.getInt("F_WAIT_TIME"));
//                    preInsert.setInt(13,resultFrom.getInt("F_FLAG"));
//                    preInsert.setInt(14,resultFrom.getInt("F_HEARTBEAT"));
//                    preInsert.setInt(15,resultFrom.getInt("F_USE"));
//                    preInsert.setTimestamp(16,resultFrom.getTimestamp("F_HEARTBEAT_TIME"));
//                    preInsert.setTimestamp(17,resultFrom.getTimestamp("F_LOGIN_TIME"));
//                    preInsert.setTimestamp(18,resultFrom.getTimestamp("F_LOST_CONNET_TIME"));
//                    preInsert.setString(19,resultFrom.getString("F_GATEWAY_VER"));
//                    preInsert.setString(20,resultFrom.getString("F_ZD_IP"));
//                    preInsert.setString(21,resultFrom.getString("F_ZD_MASK"));
//                    preInsert.setString(22,resultFrom.getString("F_ZD_GATEWAY"));
//                    preInsert.setString(23,resultFrom.getString("F_ZD_MAC"));
//                    preInsert.setString(24,resultFrom.getString("F_ZD_LCD_PASSWORD"));
//                    preInsert.setInt(25,resultFrom.getInt("F_SERVER_FLAG"));
//                    preInsert.addBatch();
//                }
//            }
//            preFrom.close();
//            preTo.close();
//            preInsert.executeBatch();
//            preUpdate.executeBatch();
//            preInsert.close();
//            preUpdate.close();
//            System.out.println("网关更新成功！");
//        }catch (Exception e){
//            e.printStackTrace();
//        }
//    }

    public void gatewayMonitor(){
        try {
            pre = con.prepareStatement("select a.F_UUID,a.F_ADDRESS,a.F_HEARTBEAT,(sysdate - F_HEARTBEAT_TIME) AS TIME_INTERVAL,to_char(F_HEARTBEAT_TIME,'yyyy-MM-dd HH24:mi:SS') AS NEWEST_HEARTBEAT_TIME," +
                    " to_char(F_LOGIN_TIME,'yyyy-MM-dd HH24:mi:SS') AS LOGIN_TIME,to_char(F_LOST_CONNET_TIME,'yyyy-MM-dd HH24:mi:SS') AS LOST_CONNET_TIME,t.F_BUILDGROUPNAME,e.F_BUILDNAME,f.F_NAME AS FLOOR,g.F_NAME AS ROOM " +
                    " from T_BE_GATEWAY a,T_BE_EQUIPMENTLIST b,T_BD_BUILDGROUPRELAINFO h,T_BD_BUILDGROUPBASEINFO t,T_BD_BUILDBASEINFO e,T_BD_FLOOR f,T_BD_ROOM g " +
                    " where a.F_UUID=b.F_UUID AND b.F_BUILDID=e.F_BUILDID(+) AND b.F_FLOORID=f.F_ID(+) AND b.F_ROOMID=g.F_ID(+) AND h.F_BUILDID(+)=b.F_BUILDID AND t.F_BUILDGROUPID(+)=h.F_BUILDGROUPID AND a.F_USE=1");
            result = pre.executeQuery();
            PreparedStatement pre1 = con.prepareStatement("insert into T_EC_EQUIPALERT_RECORD(F_UUID,F_EQUIPUUID,F_INSERTTIME,F_TYPE,F_STATUS,F_REMARK) VALUES (sys_guid(),?,sysdate,?,?,?)");
            while(result.next()) {
                if(!gatewayMap.containsKey(result.getString("F_UUID"))){
                    gatewayMap.put(result.getString("F_UUID"),false);
                }
                if(result.getDouble("TIME_INTERVAL")*1440>result.getDouble("F_HEARTBEAT")+1){
                    if(!gatewayMap.get(result.getString("F_UUID"))){
                        gatewayMap.put(result.getString("F_UUID"),true);
                        pre1.setString(1,result.getString("F_UUID"));
                        pre1.setInt(2,1);
                        pre1.setInt(3,1);
                        pre1.setString(4,"离线时间："+result.getString("LOST_CONNET_TIME"));
                        pre1.addBatch();
                    }
                }
                else{
                    if(gatewayMap.get(result.getString("F_UUID"))){
                        gatewayMap.put(result.getString("F_UUID"),false);
                        pre1.setString(1,result.getString("F_UUID"));
                        pre1.setInt(2,1);
                        pre1.setInt(3,0);
                        pre1.setString(4,"上线时间："+result.getString("LOGIN_TIME"));
                        pre1.addBatch();
                    }
                }
            }
            pre1.executeBatch();
            pre1.close();
            pre.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }
    private void ammeterMonitor(){
        try {
            pre = con.prepareStatement("select f_uuid,F_GATEWAYS_UUID,f_newest_valid_data,F_NEWEST_DATA,to_char(F_NEWEST_OPERATE_TIME,'yyyy-MM-dd HH24:mi:SS') as time,(sysdate-F_NEWEST_OPERATE_TIME) AS time_interval,F_EQUIPMENT_STATUE,F_REMARKINFO FROM T_BE_AMMETER where f_use=1");
            result = pre.executeQuery();
            PreparedStatement pre1 = con.prepareStatement("insert into T_EC_EQUIPALERT_RECORD(F_UUID,F_EQUIPUUID,F_INSERTTIME,F_TYPE,F_STATUS,F_REMARK) VALUES (sys_guid(),?,sysdate,?,?,?)");
            while(result.next()) {
                if(!gatewayMap.containsKey(result.getString("F_GATEWAYS_UUID"))||gatewayMap.get(result.getString("F_GATEWAYS_UUID"))) continue;
                if(!ammeterMap.containsKey(result.getString("F_UUID"))){
                    ammeterMap.put(result.getString("F_UUID"),false);
                }
                if(result.getDouble("F_NEWEST_DATA")==-1&&result.getInt("F_EQUIPMENT_STATUE")>3){
                    if(!ammeterMap.get(result.getString("F_UUID"))){
                        ammeterMap.put(result.getString("F_UUID"),true);
                        pre1.setString(1,result.getString("F_UUID"));
                        pre1.setInt(2,2);
                        pre1.setInt(3,2);
                        pre1.setString(4,"无法采集数据");
                        pre1.addBatch();
                    }
                }
                else if(result.getDouble("F_NEWEST_DATA")==-2||result.getDouble("TIME_INTERVAL") >= 0.0208){
                    if(!ammeterMap.get(result.getString("F_UUID"))){
                        ammeterMap.put(result.getString("F_UUID"),true);
                        pre1.setString(1,result.getString("F_UUID"));
                        pre1.setInt(2,2);
                        pre1.setInt(3,1);
                        pre1.setString(4,"最近抄表时间："+result.getString("time"));
                        pre1.addBatch();
                    }
                }
                else if(result.getDouble("F_NEWEST_VALID_DATA")>result.getDouble("F_NEWEST_DATA")&&result.getDouble("F_NEWEST_DATA")!=-1&&result.getInt("F_EQUIPMENT_STATUE")==0){
                    if(ammeterMap.get(result.getString("F_UUID"))){
                        ammeterMap.put(result.getString("F_UUID"),true);
                        pre1.setString(1,result.getString("F_UUID"));
                        pre1.setInt(2,2);
                        pre1.setInt(3,3);
                        pre1.setString(4,"底度未设置");
                        pre1.addBatch();
                    }
                }
                else{
                    if(ammeterMap.get(result.getString("F_UUID"))){
                        ammeterMap.put(result.getString("F_UUID"),false);
                        pre1.setString(1,result.getString("F_UUID"));
                        pre1.setInt(2,2);
                        pre1.setInt(3,0);
                        pre1.setString(4,"正常抄表时间："+result.getString("time"));
                        pre1.addBatch();
                    }
                }
            }
            pre1.executeBatch();
            con.commit();
            pre1.close();
            pre.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }
    private void watermeterMonitor(){
        try {
            pre = con.prepareStatement("select f_uuid,F_GATEWAYS_UUID,f_newest_valid_data,F_NEWEST_DATA,to_char(F_NEWEST_OPERATE_TIME,'yyyy-MM-dd HH24:mi:SS') as time,(sysdate-F_NEWEST_OPERATE_TIME) AS time_interval,F_EQUIPMENT_STATUE,F_REMARKINFO FROM T_BE_WATERMETER where f_use=1");
            result = pre.executeQuery();
            PreparedStatement pre1 = con.prepareStatement("insert into T_EC_EQUIPALERT_RECORD(F_UUID,F_EQUIPUUID,F_INSERTTIME,F_TYPE,F_STATUS,F_REMARK) VALUES (sys_guid(),?,sysdate,?,?,?)");
            while(result.next()) {
                if(!gatewayMap.containsKey(result.getString("F_GATEWAYS_UUID"))||gatewayMap.get(result.getString("F_GATEWAYS_UUID"))) continue;
                if(!watermeterMap.containsKey(result.getString("F_UUID"))){
                    watermeterMap.put(result.getString("F_UUID"),false);
                }
                if(result.getDouble("F_NEWEST_DATA")==-1&&result.getInt("F_EQUIPMENT_STATUE")>3){
                    if(!watermeterMap.get(result.getString("F_UUID"))){
                        watermeterMap.put(result.getString("F_UUID"),true);
                        pre1.setString(1,result.getString("F_UUID"));
                        pre1.setInt(2,3);
                        pre1.setInt(3,2);
                        pre1.setString(4,"无法采集数据");
                        pre1.addBatch();
                    }
                }
                else if(result.getDouble("F_NEWEST_DATA")==-2||result.getDouble("TIME_INTERVAL") >= 0.0208){
                    if(!watermeterMap.get(result.getString("F_UUID"))){
                        watermeterMap.put(result.getString("F_UUID"),true);
                        pre1.setString(1,result.getString("F_UUID"));
                        pre1.setInt(2,3);
                        pre1.setInt(3,1);
                        pre1.setString(4,"最近抄表时间："+result.getString("time"));
                        pre1.addBatch();
                    }
                }
                else{
                    if(watermeterMap.get(result.getString("F_UUID"))){
                        watermeterMap.put(result.getString("F_UUID"),false);
                        pre1.setString(1,result.getString("F_UUID"));
                        pre1.setInt(2,3);
                        pre1.setInt(3,0);
                        pre1.setString(4,"正常抄表时间："+result.getString("time"));
                        pre1.addBatch();
                    }
                }
            }
            pre1.executeBatch();
            con.commit();
            pre1.close();
            pre.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }
    private void sendMessage(){
        Calendar calendar=Calendar.getInstance();
        int nowhour=calendar.get(Calendar.HOUR_OF_DAY);
        int nowminute=calendar.get(Calendar.MINUTE);
        if(nowhour==8&&nowminute>=0&&nowminute<15){
            String content="今日设备故障日志如下:详情请登录节能系统中运维管理查看\n";

            System.out.println(content);
            emailService.sendMail("今日设备检测状态", mailArray, content);
        }
    }
    @Override
    public void run() {
        //getMailPhone();
        gatewayMonitor();
        ammeterMonitor();
        watermeterMonitor();
        //sendMessage();
    }
}
