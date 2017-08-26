package com.wintoo.monitor;

import lombok.Data;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcOperations;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Jason on 15/11/27.
 */
public class PreProcessMonitor implements Runnable {

    @Data
    private class TypeData{
        private Timestamp dataTime;
        private BigDecimal data;
    }

    private JdbcOperations jdbcTemplateMysql;

    private Map<String,Map<BigDecimal,TypeData>> equipData=new HashMap<>();


    public PreProcessMonitor(JdbcOperations jdbcTemplateMysql){
        this.jdbcTemplateMysql=jdbcTemplateMysql;
    }

    private void getEquipData(){
        List<Map<String,Object>> maps = jdbcTemplateMysql.queryForList("SELECT F_UUID , F_NEWEST_VALID_DATA , F_NEWEST_VALID_DATA_TIME , F_HOUR_VALID_DATA , F_HOUR_VALID_DATA_TIME , F_DAY_VALID_DATA , F_DAY_VALID_DATA_TIME , F_MON_VALID_DATA , F_MON_VALID_DATA_TIME FROM T_BE_EQUPMENTSTATUS");
        maps.forEach(map->{
            String uuid=(String) map.get("F_UUID");
            if(!equipData.containsKey(uuid)) equipData.put(uuid,new HashMap<>());
            if(map.get("F_NEWEST_VALID_DATA")!=null)
                equipData.get(uuid).put(new BigDecimal(1), new TypeData().setData((BigDecimal) map.get("F_NEWEST_VALID_DATA")).setDataTime((Timestamp) map.get("F_NEWEST_VALID_DATA_TIME")));
            if(map.get("F_DAY_VALID_DATA")!=null)
                equipData.get(uuid).put(new BigDecimal(2),new TypeData().setData((BigDecimal) map.get("F_DAY_VALID_DATA")).setDataTime((Timestamp)map.get("F_DAY_VALID_DATA_TIME")));
            if(map.get("F_MON_VALID_DATA")!=null)
                equipData.get(uuid).put(new BigDecimal(3),new TypeData().setData((BigDecimal) map.get("F_MON_VALID_DATA")).setDataTime((Timestamp)map.get("F_MON_VALID_DATA_TIME")));
        });
    }

    private void saveEquipData(){
        equipData.forEach((k,v)->{
            List<Map<String,Object>> maps=jdbcTemplateMysql.queryForList("select * from T_BE_EQUPMENTSTATUS where f_uuid=?",new Object[]{k});
            String sql;
            if(maps.isEmpty())
                sql="INSERT INTO T_BE_EQUPMENTSTATUS( F_NEWEST_VALID_DATA , F_NEWEST_VALID_DATA_TIME , F_DAY_VALID_DATA , F_DAY_VALID_DATA_TIME , F_MON_VALID_DATA , F_MON_VALID_DATA_TIME,F_UUID ) VALUES (?,?,?,?,?,?,?) ";
            else
                sql="UPDATE T_BE_EQUPMENTSTATUS SET  F_NEWEST_VALID_DATA=? , F_NEWEST_VALID_DATA_TIME=? , F_DAY_VALID_DATA=? , F_DAY_VALID_DATA_TIME=? , F_MON_VALID_DATA=? , F_MON_VALID_DATA_TIME=? WHERE F_UUID=? ";
            TypeData data15=v.get(new BigDecimal(1));
            if(data15==null) data15=new TypeData();
            TypeData dataDay=v.get(new BigDecimal(2));
            if(dataDay==null) dataDay=new TypeData();
            TypeData dataMon=v.get(new BigDecimal(3));
            if(dataMon==null) dataMon=new TypeData();
            jdbcTemplateMysql.update(sql,new Object[]{data15.getData(),data15.getDataTime(),dataDay.getData(),dataDay.getDataTime(),dataMon.getData(),dataMon.getDataTime(),k});
        });
    }

    private void saveBadData(Map<String,Object> data){
        if(data.containsKey("F_FLOW")) {
            String sql="INSERT INTO T_EA_BAD_ENERGY_W(F_UUID,F_NODES_UUID,F_CONSUM,F_FLOW,F_U,F_DATA_TIME,F_TYPE,F_SEQUENCE) VALUES(?,?,?,?,?,?,?,?)";
            jdbcTemplateMysql.update(sql,new Object[]{data.get("F_UUID"),data.get("F_NODES_UUID"),data.get("DATA"),data.get("F_FLOW"),data.get("F_U"),data.get("F_DATA_TIME"),data.get("F_TYPE"),data.get("F_SEQUENCE")});
        }
        else {
            String sql = "INSERT INTO T_EA_BAD_ENERGY(F_UUID,F_NODES_UUID,F_ACTIVE,F_REACTIVE,F_DATA_TIME,F_TYPE,F_SEQUENCE) VALUES(?,?,?,?,?,?,?)";
            jdbcTemplateMysql.update(sql,new Object[]{data.get("F_UUID"),data.get("F_NODES_UUID"),data.get("DATA"),data.get("F_REACTIVE"),data.get("F_DATA_TIME"),data.get("F_TYPE"),data.get("F_SEQUENCE")});
        }
    }

    private List<Map<String,Object>> get15MinValue(List<Map<String,Object>> dataMaps){
        List<Map<String,Object>> result=new ArrayList<>();
        dataMaps.forEach(map ->{
            String uuid=(String) map.get("F_NODES_UUID");
            BigDecimal type=(BigDecimal) map.get("F_TYPE");
            BigDecimal data=(BigDecimal) map.get("DATA");
            Timestamp dataTime=(Timestamp)map.get("F_DATA_TIME");
            if(data.compareTo(new BigDecimal(0))<0){
                saveBadData(map);
                return;
            }
            long time;
            if(type.equals(new BigDecimal(1))) time=15*60*1000;
            else if(type.equals(new BigDecimal(2))) time=24*60*60*1000;
            else if(type.equals(new BigDecimal(3))) time=30*24*60*60*1000;
            else return;
            Map<BigDecimal,TypeData> dataMap=equipData.get(uuid);
            if(dataMap==null){
                equipData.put(uuid,new HashMap<>());
                TypeData typeData=new TypeData();
                typeData.setData(data).setDataTime(dataTime);
                equipData.get(uuid).put(type,typeData);
                return;
            }
            TypeData typeData=dataMap.get(type);
            if(typeData==null){
                typeData=new TypeData();
                typeData.setData(data).setDataTime(dataTime);
                dataMap.put(type,typeData);
                return;
            }
            long times=(dataTime.getTime()-typeData.getDataTime().getTime())/time;
            if(times<=0) return;
            BigDecimal interData=data.subtract(typeData.getData()).divide(BigDecimal.valueOf(times),4,BigDecimal.ROUND_HALF_UP);
            if(interData.compareTo(new BigDecimal(0))<0||interData.compareTo(new BigDecimal(100))>0){
                saveBadData(map);
                if(Math.abs(dataTime.getTime()-typeData.getDataTime().getTime())<=30*24*60*60*1000);
                    typeData.setData(data).setDataTime(dataTime);
                return;
            }
            for(int i=0;i<times;i++){
                data=data.add(interData);
                dataTime.setTime(dataTime.getTime()+time);
                Map<String,Object> data15=new HashMap<>();
                map.forEach((k,v)->data15.put(k,v));
                data15.put("DATA",BigDecimal.valueOf(data.doubleValue()));
                data15.put("F_TIME_INTERVEL_ACTIVE",interData);
                data15.put("F_DATA_TIME",new Timestamp(dataTime.getTime()));
                result.add(data15);
            }
            typeData.setData(data).setDataTime(dataTime);
        });

        return result;
    }

    private boolean transAmData(){

        BigDecimal maxNum=new BigDecimal(0);
        boolean hasData=false;
        try {
            Map map=jdbcTemplateMysql.queryForMap("SELECT max(F_SEQUENCE) maxNum from T_EA_ENERGYHISTORY");
            if(map.get("maxNum")!=null) maxNum=(BigDecimal) map.get("maxNum");
            System.out.println("电量序号起始："+maxNum);
            List<Map<String,Object>> maps = jdbcTemplateMysql.queryForList("SELECT F_UUID,F_NODES_UUID,F_ACTIVE DATA,F_REACTIVE,F_DATA_TIME,F_TYPE,F_IS_NEW,F_INSERT_TIME,F_P,F_PA,F_PB,F_PC,F_NP,F_NPA,F_NPB,F_NPC,F_UA,F_UB,F_UC,F_IA,F_IB,F_IC,F_GLYS,F_GLYSA,F_GLYSB,F_GLYSC,F_SEQUENCE from T_NODE_DATA where f_sequence>?  AND ROWNUM<=2000 order by F_SEQUENCE",new Object[]{maxNum});
            jdbcTemplateMysql.batchUpdate("INSERT INTO T_EA_ENERGYHISTORY(F_UUID,F_NODES_UUID,F_ACTIVE,F_REACTIVE,F_DATA_TIME,F_TYPE,F_SEQUENCE) " +
                    "VALUES(?,?,?,?,?,?,?)", new BatchPreparedStatementSetter() {
                @Override
                public void setValues(PreparedStatement preparedStatement, int i) throws SQLException {
                    preparedStatement.setString(1,(String) maps.get(i).get("F_UUID"));
                    preparedStatement.setString(2,(String) maps.get(i).get("F_NODES_UUID"));
                    preparedStatement.setBigDecimal(3,(BigDecimal) maps.get(i).get("DATA"));
                    preparedStatement.setBigDecimal(4,(BigDecimal) maps.get(i).get("F_REACTIVE"));
                    preparedStatement.setTimestamp(5,(Timestamp) maps.get(i).get("F_DATA_TIME"));
                    preparedStatement.setBigDecimal(6,(BigDecimal) maps.get(i).get("F_TYPE"));
                    preparedStatement.setBigDecimal(7,(BigDecimal) maps.get(i).get("F_SEQUENCE"));
                }
                @Override
                public int getBatchSize() {
                    return maps.size();
                }
            });
            List<Map<String,Object>> map15 = get15MinValue(maps);
            jdbcTemplateMysql.batchUpdate("INSERT INTO T_EA_15_ENERGY_BUFFER(F_DEVICECODE,F_ACTIVE,F_REACTIVE,F_DATATIME,F_TYPE,F_TIME_INTERVEL_ACTIVE) " +
                    "VALUES(?,?,?,?,?,?)", new BatchPreparedStatementSetter() {
                @Override
                public void setValues(PreparedStatement preparedStatement, int i) throws SQLException {
                    preparedStatement.setString(1,(String) map15.get(i).get("F_NODES_UUID"));
                    preparedStatement.setBigDecimal(2,(BigDecimal) map15.get(i).get("DATA"));
                    preparedStatement.setBigDecimal(3,(BigDecimal) map15.get(i).get("F_REACTIVE"));
                    preparedStatement.setTimestamp(4,(Timestamp) map15.get(i).get("F_DATA_TIME"));
                    preparedStatement.setBigDecimal(5,(BigDecimal) map15.get(i).get("F_TYPE"));
                    preparedStatement.setBigDecimal(6,(BigDecimal) map15.get(i).get("F_TIME_INTERVEL_ACTIVE"));
                }
                @Override
                public int getBatchSize() {
                    return map15.size();
                }
            });
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return hasData;
    }

    private boolean transWaterData(){
        BigDecimal maxNum=new BigDecimal(0);
        boolean hasData=false;
        try {
            Map map = jdbcTemplateMysql.queryForMap("SELECT max(F_SEQUENCE) maxNum from T_EA_ENERGYHISTORY_W");
            if(map.get("maxNum")!=null) maxNum=(BigDecimal) map.get("maxNum");
            System.out.println("水量序号起始："+maxNum);
            List<Map<String,Object>> maps=jdbcTemplateMysql.queryForList("SELECT F_UUID,F_NODES_UUID,F_CONSUM DATA,F_FLOW,F_DATA_TIME,F_TYPE,F_IS_NEW,F_INSERT_TIME,F_U,F_SEQUENCE from T_W_NODE_DATA where f_sequence>? AND ROWNUM<=2000 order by F_SEQUENCE",new Object[]{maxNum});
            jdbcTemplateMysql.batchUpdate("INSERT INTO T_EA_ENERGYHISTORY_W(F_UUID,F_NODES_UUID,F_CONSUM,F_FLOW,F_U,F_DATA_TIME,F_TYPE,F_SEQUENCE) " +
                    "VALUES(?,?,?,?,?,?,?,?)", new BatchPreparedStatementSetter() {
                @Override
                public void setValues(PreparedStatement preparedStatement, int i) throws SQLException {
                    preparedStatement.setString(1,(String) maps.get(i).get("F_UUID"));
                    preparedStatement.setString(2,(String) maps.get(i).get("F_NODES_UUID"));
                    preparedStatement.setBigDecimal(3,(BigDecimal) maps.get(i).get("DATA"));
                    preparedStatement.setBigDecimal(4,(BigDecimal) maps.get(i).get("F_FLOW"));
                    preparedStatement.setBigDecimal(5,(BigDecimal) maps.get(i).get("F_U"));
                    preparedStatement.setTimestamp(6,(Timestamp) maps.get(i).get("F_DATA_TIME"));
                    preparedStatement.setBigDecimal(7,(BigDecimal) maps.get(i).get("F_TYPE"));
                    preparedStatement.setBigDecimal(8,(BigDecimal) maps.get(i).get("F_SEQUENCE"));
                }
                @Override
                public int getBatchSize() {
                    return maps.size();
                }
            });
            List<Map<String,Object>> map15=get15MinValue(maps);
            jdbcTemplateMysql.batchUpdate("INSERT INTO T_EA_15_ENERGY_W_BUFFER(F_DEVICECODE,F_CONSUM,F_FLOW,F_U,F_DATA_TIME,F_TYPE,F_TIME_INTERVEL_ACTIVE) " +
                    "VALUES(?,?,?,?,?,?,?)", new BatchPreparedStatementSetter() {
                @Override
                public void setValues(PreparedStatement preparedStatement, int i) throws SQLException {
                    preparedStatement.setString(1,(String) map15.get(i).get("F_NODES_UUID"));
                    preparedStatement.setBigDecimal(2,(BigDecimal) map15.get(i).get("DATA"));
                    preparedStatement.setBigDecimal(3,(BigDecimal) map15.get(i).get("F_FLOW"));
                    preparedStatement.setBigDecimal(4,(BigDecimal) map15.get(i).get("F_U"));
                    preparedStatement.setTimestamp(5,(Timestamp) map15.get(i).get("F_DATA_TIME"));
                    preparedStatement.setBigDecimal(6,(BigDecimal) map15.get(i).get("F_TYPE"));
                    preparedStatement.setBigDecimal(7,(BigDecimal) map15.get(i).get("F_TIME_INTERVEL_ACTIVE"));
                }
                @Override
                public int getBatchSize() {
                    return map15.size();
                }
            });
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return hasData;
    }

    @Override
    public void run() {
        long start=System.currentTimeMillis();
        getEquipData();
        transAmData();
        transWaterData();
        saveEquipData();
        System.out.println("本次传输结束!耗时：" + (System.currentTimeMillis() - start));
    }
}
