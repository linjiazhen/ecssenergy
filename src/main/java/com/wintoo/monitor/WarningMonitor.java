package com.wintoo.monitor;

import com.wintoo.model.*;
import com.wintoo.tools.*;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Haizi on 15/10/16.
 * 用于规则监测，发现违规情况写入到表：T_EC_VIOLATION_RECORD。
 */
public class WarningMonitor implements Runnable {
    private Connection con = null;
    private Map<EnergyKey,EnergyAlert> rule = new HashMap<>();
    private Map<String,String> contactMap=new HashMap<>();
    private double percent;
    private SmsApi smsApi=new SmsApi();
    private DecimalFormat df=new java.text.DecimalFormat("#.##");

    private SimpleDateFormat daysdf = new SimpleDateFormat("yyyy/MM/dd");
    private SimpleDateFormat monsdf = new SimpleDateFormat("yyyy/MM");
    private SimpleDateFormat yearsdf = new SimpleDateFormat("yyyy");

    public  WarningMonitor(Connection con) {
        try {
            this.con=con;
        } catch (Exception e) {
            System.out.println(e);
        }
    }
    private void setRule(String item,String item1,String item2,String item3,String timeunit,String energyitem,double value){
        EnergyKey energyKey=new EnergyKey();
        energyKey.setId(item);
        energyKey.setEnergytype(energyitem);
        energyKey.setDatetime(timeunit);
        EnergyAlert energyAlert=new EnergyAlert();
        energyAlert.setItem1(item1);
        energyAlert.setItem2(item2);
        energyAlert.setItem3(item3);
        energyAlert.setEnergyitem(energyitem);
        energyAlert.setTimeunit(timeunit);
        energyAlert.setValue(value);
        if(!rule.containsKey(energyKey))
            rule.put(energyKey,energyAlert);
    }
    private void init(){
        rule.clear();
        try {
            PreparedStatement  pre = con.prepareStatement("select F_ITEM1, F_ITEM2, F_ITEM3, F_LEVEL, F_ENERGYITEM, F_TIMEUNIT, F_VALUE FROM T_EC_ENERGY_RULE where F_STATE = 1 ORDER BY F_TIME DESC");
            ResultSet result = pre.executeQuery();
            while(result.next()) {
                String item1 = result.getString("F_ITEM1");
                String item2 = result.getString("F_ITEM2");
                String item3 = result.getString("F_ITEM3");
                String level = result.getString("F_LEVEL");
                String energyitem = result.getString("F_ENERGYITEM");
                String timeunit = result.getString("F_TIMEUNIT");
                double value = result.getDouble("F_VALUE");
                if(level.equals("third")){
                    if(!item3.equals("#")) {
                        setRule(item3,item1,item2,item3,timeunit,energyitem,value);
                    }
                    else
                        if(item3.equals("#")&&!item2.equals("#")){
                            String sql="SELECT F_ID FROM T_BO_ORGAN WHERE F_PID='"+item2+"'";
                            PreparedStatement  pre1 = con.prepareStatement(sql);
                            ResultSet result1 = pre1.executeQuery();
                            while(result1.next()) {
                                setRule(result1.getString("F_ID"),item1,item2,result1.getString("F_ID"),timeunit,energyitem,value);
                            }
                            pre1.close();
                        }
                        else{
                            String sql="SELECT a.F_ID,b.F_ID FROM T_BO_ORGAN a,T_BO_ORGAN b WHERE a.F_PID=b.F_ID AND b.F_PID='"+item1+"'";
                            PreparedStatement  pre1 = con.prepareStatement(sql);
                            ResultSet result1 = pre1.executeQuery();
                            while(result1.next()) {
                                setRule(result1.getString(1),item1,result1.getString(2),result1.getString(1),timeunit,energyitem,value);
                            }
                            pre1.close();
                        }
                }
                else if(level.equals("second")) {
                    if (!item2.equals("#")) {
                        setRule(item2,item1,item2,item3,timeunit,energyitem,value);
                    } else if (item2.equals("#") && !item1.equals("#")) {
                        String sql = "SELECT F_ID FROM T_BO_ORGAN WHERE F_PID='" + item1+"'";
                        PreparedStatement pre1 = con.prepareStatement(sql);
                        ResultSet result1 = pre1.executeQuery();
                        while (result1.next()) {
                            setRule(result1.getString("F_ID"),item1,result1.getString("F_ID"),item3,timeunit,energyitem,value);
                        }
                        pre1.close();
                    }
                }
                else{
                    setRule(item1,item1,item2,item3,timeunit,energyitem,value);
                }
            }
            pre.close();
            contactMap.clear();
            pre = con.prepareStatement("SELECT F_ID,F_TEL,F_EMAIL FROM T_BO_ORGAN WHERE f_tel is not NULL ");
            result = pre.executeQuery();
            while(result.next()) {
                contactMap.put(result.getString(1),result.getString(2)+","+result.getString(3));
            }
            pre.close();
            pre = con.prepareStatement("SELECT F_PERCENT FROM T_EC_REMINDWAY");
            result = pre.executeQuery();
            while(result.next()) {
                percent=result.getDouble(1)/100.0;
            }
            pre.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    private void saveAlert(EnergyKey energyKey,String time,double usage){
        EnergyAlert energyAlert;
        if(rule.containsKey(energyKey)){
            energyAlert=rule.get(energyKey);
            if(usage!=energyAlert.getUsage()&&usage>=energyAlert.getValue()*percent){
                energyAlert.setUsage(usage);
                energyAlert.setDatetime(time);
            }
            else return;
        }
        else return;
        String sql ;
        try {
            if(energyAlert.getTimeunit().equals("day"))
                sql = "INSERT INTO T_EC_VIOLATION_RECORD(F_UUID,F_ITEM1,F_ITEM2,F_ITEM3,F_ENERGYITEM,F_TIMEUNIT,F_DATETIME,F_VALUE,F_USAGE) VALUES(sys_guid(),?,?,?,?,?,to_date(?,'yyyy/mm/dd'),?,?)";
            else
            if(energyAlert.getTimeunit().equals("month"))
                sql = "INSERT INTO T_EC_VIOLATION_RECORD(F_UUID,F_ITEM1,F_ITEM2,F_ITEM3,F_ENERGYITEM,F_TIMEUNIT,F_DATETIME,F_VALUE,F_USAGE) VALUES(sys_guid(),?,?,?,?,?,to_date(?,'yyyy/mm'),?,?)";
            else
                sql = "INSERT INTO T_EC_VIOLATION_RECORD(F_UUID,F_ITEM1,F_ITEM2,F_ITEM3,F_ENERGYITEM,F_TIMEUNIT,F_DATETIME,F_VALUE,F_USAGE) VALUES(sys_guid(),?,?,?,?,?,to_date(?,'yyyy'),?,?)";
            PreparedStatement stmt = con.prepareStatement(sql);
            stmt.setString(1,energyAlert.getItem1());
            stmt.setString(2,energyAlert.getItem2());
            stmt.setString(3,energyAlert.getItem3());
            stmt.setString(4,energyAlert.getEnergyitem());
            stmt.setString(5,energyAlert.getTimeunit());
            stmt.setString(6,energyAlert.getDatetime());
            stmt.setDouble(7,energyAlert.getValue());
            stmt.setDouble(8,energyAlert.getUsage());
            stmt.executeUpdate();
            stmt.close();
        }catch (Exception e) {
            //e.printStackTrace();
            try {
                if(energyAlert.getTimeunit().equals("day"))
                    sql = "UPDATE T_EC_VIOLATION_RECORD SET F_VALUE=?,F_USAGE=? WHERE F_ITEM1=? AND F_ITEM2=? AND F_ITEM3=? AND F_ENERGYITEM=? AND F_DATETIME=to_date(?,'yyyy/mm/dd')";
                else
                if(energyAlert.getTimeunit().equals("month"))
                    sql = "UPDATE T_EC_VIOLATION_RECORD SET F_VALUE=?,F_USAGE=? WHERE F_ITEM1=? AND F_ITEM2=? AND F_ITEM3=? AND F_ENERGYITEM=? AND F_DATETIME=to_date(?,'yyyy/mm')";
                else
                    sql = "UPDATE T_EC_VIOLATION_RECORD SET F_VALUE=?,F_USAGE=? WHERE F_ITEM1=? AND F_ITEM2=? AND F_ITEM3=? AND F_ENERGYITEM=? AND F_DATETIME=to_date(?,'yyyy')";
                PreparedStatement stmt = con.prepareStatement(sql);
                stmt.setDouble(1, energyAlert.getValue());
                stmt.setDouble(2, energyAlert.getUsage());
                stmt.setString(3, energyAlert.getItem1());
                stmt.setString(4, energyAlert.getItem2());
                stmt.setString(5, energyAlert.getItem3());
                stmt.setString(6, energyAlert.getEnergyitem());
                stmt.setString(7, energyAlert.getDatetime());
                stmt.executeUpdate();
                con.commit();
                stmt.close();
            } catch (Exception e1) {
                e.printStackTrace();
            }
        }

    }
    private void checkAlert(Calendar calendar){
        try {
            String time=daysdf.format(calendar.getTime());
           // System.out.println(time);
            String sql = "SELECT F_ORGANID,F_ENERGYITEMCODE,F_VALUE from T_EC_ORGAN_DAY WHERE F_STARTTIME=TO_DATE('"+time+"','yyyy/mm/dd')";
            PreparedStatement pre = con.prepareStatement(sql);
            ResultSet result = pre.executeQuery();
            while (result.next()){
                EnergyKey energyKey = new EnergyKey();
                energyKey.setId(result.getString("F_ORGANID"));
                energyKey.setEnergytype(result.getString("F_ENERGYITEMCODE"));
                energyKey.setDatetime("day");
                saveAlert(energyKey,time,result.getDouble("F_VALUE"));
            }
            pre.close();
            time=monsdf.format(calendar.getTime());
            sql = "SELECT F_ORGANID,F_ENERGYITEMCODE,F_VALUE from T_EC_ORGAN_MON WHERE F_STARTTIME=TO_DATE('"+time+"','yyyy/mm')";
            pre = con.prepareStatement(sql);
            result = pre.executeQuery();
            while (result.next()){
                EnergyKey energyKey = new EnergyKey();
                energyKey.setId(result.getString("F_ORGANID"));
                energyKey.setEnergytype(result.getString("F_ENERGYITEMCODE"));
                energyKey.setDatetime("month");
                saveAlert(energyKey,time,result.getDouble("F_VALUE"));
            }
            pre.close();
            time=yearsdf.format(calendar.getTime());
            sql = "SELECT F_ORGANID,F_ENERGYITEMCODE,F_VALUE from T_EC_ORGAN_YEAR WHERE F_STARTTIME=TO_DATE('"+time+"','yyyy')";
            pre = con.prepareStatement(sql);
            result = pre.executeQuery();
            while (result.next()){
                EnergyKey energyKey = new EnergyKey();
                energyKey.setId(result.getString("F_ORGANID"));
                energyKey.setEnergytype(result.getString("F_ENERGYITEMCODE"));
                energyKey.setDatetime("year");
                saveAlert(energyKey,time,result.getDouble("F_VALUE"));
            }
            pre.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }
    private void sendMessage(){
        String message="";
        try {
            String sql = "select a.F_ITEM1,a.F_ITEM2,a.F_ITEM3,a.F_UUID,a.F_TIMEUNIT,a.F_DATETIME,a.F_VALUE,a.F_USAGE,a.F_STATUS,b.F_ENERGYITEMNAME,NVL(c.F_NAME,'') as first,NVL(d.F_NAME,'') as second,NVL(e.F_NAME,'') as third " +
                    "from T_EC_VIOLATION_RECORD a,T_DT_ENERGYITEMDICT b,T_BO_ORGAN c,T_BO_ORGAN d,T_BO_ORGAN e " +
                    "WHERE  a.F_ENERGYITEM=b.F_ENERGYITEMCODE AND a.F_ITEM1=c.f_id(+) and a.F_ITEM2=d.F_ID(+) and a.F_ITEM3=e.F_ID(+) AND a.F_STATUS!=2";
            PreparedStatement pre = con.prepareStatement(sql);
            ResultSet result = pre.executeQuery();
            while (result.next()) {
                String organid=!result.getString("F_ITEM3").equals("#")?result.getString("F_ITEM3"):(!result.getString("F_ITEM2").equals("#")?result.getString("F_ITEM2"):result.getString("F_ITEM1"));
                if(contactMap.containsKey(organid)) {
                    String mobile = contactMap.get(organid).split(",")[0];
                    if (result.getDouble("F_USAGE") >= result.getDouble("F_VALUE")) {
                        message = "您部门(" + result.getString("first") + (result.getString("second") != null ? result.getString("second") : "") + (result.getString("third") != null ? result.getString("third") : "") + ")";
                        if (result.getString("f_timeunit").equals("day"))
                            message += "本日";
                        else if (result.getString("f_timeunit").equals("month"))
                            message += "本月";
                        else
                            message += "本年";
                        message += result.getString("F_ENERGYITEMNAME") + "限额为" + result.getDouble("F_VALUE") + ",已用" + result.getDouble("F_USAGE");
                        message += ",超额" + df.format(result.getDouble("F_USAGE") - result.getDouble("F_VALUE")) + ",请注意节能！";
                        PreparedStatement  pre1 = con.prepareStatement("UPDATE T_EC_VIOLATION_RECORD SET F_STATUS=2 WHERE F_UUID='"+result.getString("F_UUID")+"'");
                        pre1.executeUpdate();
                    } else if (result.getDouble("F_USAGE") >= result.getDouble("F_VALUE") * percent && result.getInt("f_status") == 0) {
                        message = "您部门(" + result.getString("first") + (result.getString("second") != null ? result.getString("second") : "") + (result.getString("third") != null ? result.getString("third") : "") + ")";
                        if (result.getString("f_timeunit").equals("day"))
                            message += "本日";
                        else if (result.getString("f_timeunit").equals("month"))
                            message += "本月";
                        else
                            message += "本年";
                        message += result.getString("F_ENERGYITEMNAME") + "限额为" + result.getDouble("F_VALUE") + ",已用" + result.getDouble("F_USAGE");
                        message += ",已使用" + df.format(result.getDouble("F_USAGE") / result.getDouble("F_VALUE") * 100) + "％,请注意节能！";
                        PreparedStatement  pre1 = con.prepareStatement("UPDATE T_EC_VIOLATION_RECORD SET F_STATUS=1 WHERE F_UUID='"+result.getString("F_UUID")+"'");
                        pre1.executeUpdate();
                    }
                    smsApi.sendSms(message, mobile);
                    //System.out.println(message);
                }
            }
            pre.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }
    @Override
    public void run() {
        init();
        checkAlert(Calendar.getInstance());
        sendMessage();
    }
}
