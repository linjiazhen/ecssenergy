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
public class TransmitMonitor implements Runnable {


    private JdbcOperations jdbcTemplateMysql;

    private JdbcOperations jdbcTemplateOracle;


    public TransmitMonitor(JdbcOperations jdbcTemplateOracle, JdbcOperations jdbcTemplateMysql){
        this.jdbcTemplateMysql=jdbcTemplateMysql;
        this.jdbcTemplateOracle=jdbcTemplateOracle;
    }

    private boolean transAmData(){

        BigDecimal maxNum=new BigDecimal(0);
        boolean hasData=false;
        try {
            Map map=jdbcTemplateMysql.queryForMap("SELECT max(F_SEQUENCE) maxNum from T_EA_ENERGYHISTORY");
            if(map.get("maxNum")!=null) maxNum=(BigDecimal) map.get("maxNum");
            System.out.println("电量序号起始："+maxNum);
            List<Map<String,Object>> maps = jdbcTemplateOracle.queryForList("SELECT F_UUID,F_NODES_UUID,F_ACTIVE DATA,F_REACTIVE,F_DATA_TIME,F_TYPE,F_IS_NEW,F_INSERT_TIME,F_P,F_PA,F_PB,F_PC,F_NP,F_NPA,F_NPB,F_NPC,F_UA,F_UB,F_UC,F_IA,F_IB,F_IC,F_GLYS,F_GLYSA,F_GLYSB,F_GLYSC,F_SEQUENCE from T_NODE_DATA where f_sequence>?  AND ROWNUM<=2000 order by F_SEQUENCE",new Object[]{maxNum});
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
            List<Map<String,Object>> maps=jdbcTemplateOracle.queryForList("SELECT F_UUID,F_NODES_UUID,F_CONSUM DATA,F_FLOW,F_DATA_TIME,F_TYPE,F_IS_NEW,F_INSERT_TIME,F_U,F_SEQUENCE from T_W_NODE_DATA where f_sequence>? AND ROWNUM<=2000 order by F_SEQUENCE",new Object[]{maxNum});
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
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return hasData;
    }

    @Override
    public void run() {
        long start=System.currentTimeMillis();
        transAmData();
        transWaterData();
        System.out.println("本次传输结束!耗时：" + (System.currentTimeMillis() - start));
    }
}
