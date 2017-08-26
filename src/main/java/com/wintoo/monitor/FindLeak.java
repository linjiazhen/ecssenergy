package com.wintoo.monitor;

import com.wintoo.tools.*;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.regex.Pattern;

/**
 * Created by HaiziGe on 2015/10/16.
 */
public class FindLeak implements Runnable  {
    SimpleDateFormat ft = new SimpleDateFormat ("yyyy/MM/dd");
    private Connection con = null;
    private PreparedStatement pre = null;
    private ResultSet result = null;
    private EmailService emailService=new EmailService();
    private SmsApi smsApi=new SmsApi();
    private String mailArray="";
    private String phone="";
    Pattern p = Pattern.compile("\\s*|\t|\r|\n");

    public FindLeak(Connection con) {
        //数据库连接初始化部分
        try {
            this.con=con;
        } catch (Exception e) {
            System.out.println(e);
        }
    }
    private void getMailPhone()throws SQLException{
        mailArray="";
        phone="";
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
    }
    private String getMessageString() throws SQLException {
        String message="【节能监管平台】今日用能预警:\n";
        // TODO Auto-generated method stub
        Calendar calendar=Calendar.getInstance();
        boolean flag=false;
        String sql = "SELECT * FROM (select a.F_NEWEST_DATA,a.F_NEWEST_VALID_DATA,(sysdate-a.F_NEWEST_OPERATE_TIME) AS TIME_INTERVAL,a.F_EQUIPMENT_STATUE as STATUE,x.F_ADDRESS AS GATEWAY,b.F_EQUIPID AS F_EQUIP,c.F_MODEL,d.F_SUBTYPE,b.F_REMARK " +
                ",a.f_remarkinfo,a.F_USE,a.F_PN,a.F_UUID,e.energy from T_BE_WATERMETER a,T_BE_EQUIPMENTLIST b,T_BE_EQUIPMENTBATCH c,T_BE_EQUIPMENTTYPE d,T_BE_GATEWAY x, " +
                "(select a.F_DEVICECODE, sum(a.F_TIME_INTERVEL_ACTIVE) as energy from T_BE_15_ENERGY_BUFFER a,T_BE_WATERMETER b where a.F_TYPE=1 and a.F_DEVICECODE=b.F_UUID and F_DATATIME>=to_date(?,'yyyy/mm/dd hh24:mi') and F_DATATIME<to_date(?,'yyyy/mm/dd hh24:mi') " +
                "group by a.F_DEVICECODE ) e where a.F_GATEWAYS_UUID=x.F_UUID(+) AND a.F_UUID=b.F_UUID AND a.F_BATCHID=c.F_UUID AND b.F_TYPEID=d.F_UUID  AND a.F_UUID=e.f_devicecode and e.f_devicecode not in (SELECT f_id from T_EC_WHITELIST) and e.energy>? ORDER by e.energy desc) WHERE rownum<=2 " ;
        pre = con.prepareStatement(sql);
        String date=ft.format(calendar.getTime());
        pre.setString(1,date+" 01:00");
        pre.setString(2,date+" 05:00");
        pre.setInt(3,10);
        result = pre.executeQuery();
        while(result.next()) {
            if(!flag){
                flag=true;
            }
            message+=p.matcher(result.getString("f_remarkinfo")).replaceAll("")+",凌晨漏水"+(int)Math.floor(result.getDouble("energy"))+"吨";
            if(result.getDouble("energy")>1000)
                message+=",预计为系统故障\n";
            else
                message+="\n";
        }
        pre.close();
        boolean flag1=false;
        sql = "SELECT * FROM (SELECT a.F_BUILDGROUPNAME||b.F_BUILDNAME as buildname,f.* from T_BD_BUILDGROUPBASEINFO a,T_BD_BUILDBASEINFO b,T_BD_BUILDGROUPRELAINFO c," +
                "(select F_BUILDID,F_ENERGYITEMCODE,F_BUILDLEVEL,sum(F_VALUE) as energy from T_EC_BUILD_HOUR where F_BUILDLEVEL=2 and F_STARTTIME>=to_date(?,'yyyy/mm/dd hh24:mi') " +
                "and F_STARTTIME<to_date(?,'yyyy/mm/dd hh24:mi')  and F_ENERGYITEMCODE='01000' group by F_BUILDID,F_ENERGYITEMCODE,F_BUILDLEVEL ) f " +
                "where a.F_BUILDGROUPID=c.F_BUILDGROUPID and b.F_BUILDID=c.F_BUILDID and b.F_BUILDID=f.f_buildid and f.f_buildid not in (SELECT f_id from T_EC_WHITELIST) and f.energy>? ORDER by f.energy desc) WHERE rownum<=2";
        pre = con.prepareStatement(sql);
        date=ft.format(calendar.getTime());
        pre.setString(1,date+" 01:00");
        pre.setString(2,date+" 05:00");
        pre.setInt(3,10);
        result = pre.executeQuery();
        while(result.next()) {
            if(!flag1){
                flag1=true;
            }
            message+=p.matcher(result.getString("buildname")).replaceAll("")+",凌晨异常用电"+(int)Math.floor(result.getDouble("energy"))+"度";
            if(result.getDouble("energy")>10000)
                message+=",预计为系统故障\n";
            else message+="\n";
        }
        pre.close();
        sql = "SELECT * FROM (SELECT a.F_BUILDGROUPNAME||b.F_BUILDNAME||d.F_NAME as buildname,f.* from T_BD_BUILDGROUPBASEINFO a,T_BD_BUILDBASEINFO b,T_BD_BUILDGROUPRELAINFO c,T_BD_FLOOR d," +
                "(select F_BUILDID,F_ENERGYITEMCODE,F_BUILDLEVEL,sum(F_VALUE) as energy from T_EC_BUILD_HOUR where F_BUILDLEVEL=1 and F_STARTTIME>=to_date(?,'yyyy/mm/dd hh24:mi') " +
                "and F_STARTTIME<to_date(?,'yyyy/mm/dd hh24:mi')  and F_ENERGYITEMCODE='01000' group by F_BUILDID,F_ENERGYITEMCODE,F_BUILDLEVEL ) f " +
                "where a.F_BUILDGROUPID=c.F_BUILDGROUPID and b.F_BUILDID=c.F_BUILDID and b.F_BUILDID=d.F_BUILDID and d.F_ID=f.f_buildid and f.f_buildid not in (SELECT f_id from T_EC_WHITELIST) and f.energy>? ORDER by f.energy desc) WHERE rownum<=2";
        pre = con.prepareStatement(sql);
        date=ft.format(calendar.getTime());
        pre.setString(1,date+" 01:00");
        pre.setString(2,date+" 05:00");
        pre.setInt(3,10);
        result = pre.executeQuery();
        while(result.next()) {
            if(!flag1){
                flag1=true;
            }
            message+=p.matcher(result.getString("buildname")).replaceAll("")+",凌晨异常用电"+(int)Math.floor(result.getDouble("energy"))+"度";
            if(result.getDouble("energy")>1000)
                message+=",预计为系统故障\n";
            else message+="\n";
        }
        pre.close();
        sql = "SELECT * FROM (SELECT a.F_BUILDGROUPNAME||b.F_BUILDNAME||e.F_NAME as buildname,f.* from T_BD_BUILDGROUPBASEINFO a,T_BD_BUILDBASEINFO b,T_BD_BUILDGROUPRELAINFO c,T_BD_FLOOR d,T_BD_ROOM e," +
                "(select F_BUILDID,F_ENERGYITEMCODE,F_BUILDLEVEL,sum(F_VALUE) as energy from T_EC_BUILD_HOUR where F_BUILDLEVEL=0 and F_STARTTIME>=to_date(?,'yyyy/mm/dd hh24:mi') " +
                "and F_STARTTIME<to_date(?,'yyyy/mm/dd hh24:mi')  and F_ENERGYITEMCODE='01000' group by F_BUILDID,F_ENERGYITEMCODE,F_BUILDLEVEL ) f " +
                "where a.F_BUILDGROUPID=c.F_BUILDGROUPID and b.F_BUILDID=c.F_BUILDID and b.F_BUILDID=d.F_BUILDID and d.F_ID=e.F_FLOORID and e.F_ID=f.f_buildid and f.f_buildid not in (SELECT f_id from T_EC_WHITELIST) and f.energy>? ORDER by f.energy desc) WHERE rownum<=2";
        pre = con.prepareStatement(sql);
        date=ft.format(calendar.getTime());
        pre.setString(1,date+" 01:00");
        pre.setString(2,date+" 05:00");
        pre.setInt(3,10);
        result = pre.executeQuery();
        while(result.next()) {
            if(!flag1){
                flag1=true;
            }
            message+=p.matcher(result.getString("buildname")).replaceAll("")+",凌晨异常用电"+(int)Math.floor(result.getDouble("energy"))+"度";
            if(result.getDouble("energy")>1000)
                message+=",预计为系统故障\n";
            else message+="\n";
        }
        pre.close();
        if(flag||flag1)
            message+="请登录平台进行异常排查,谢谢！\n";
        else
            message+="系统运行良好没有发现漏水和电量浪费！\n";
        return message;
    }
    @Override
    public void run() {
        try {
            getMailPhone();
            String message=getMessageString();
            emailService.sendMail( "今日能耗异常需排查信息",mailArray, message);
            smsApi.sendSms(message,phone);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
