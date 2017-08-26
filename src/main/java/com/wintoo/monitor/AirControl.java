package com.wintoo.monitor;



import com.wintoo.tools.*;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Created by Jason on 15/11/27.
 */
public class AirControl implements Runnable {

    private Connection con ;
    private ModBus modbus;
    public AirControl(Connection con,ModBus modbus){
        try {
            this.con=con;
            this.modbus=modbus;
        } catch (Exception e) {
            // TODO: handle exception
            System.out.println(e);
        }
    }
    private void updateValue() throws SQLException, InterruptedException {
        PreparedStatement stmt=con.prepareStatement("UPDATE T_BC_CONTROLAIR SET F_VALUE=? WHERE F_MODBUSTYPE=? and F_MODBUSADDRESS=?");
        short[] data=modbus.readHoldingRegistersTest(1,0,75);
        for (int i=0;i<75;i++) {
            stmt.setInt(1,data[i]);
            stmt.setInt(2,3);
            stmt.setInt(3,i+1);
            stmt.addBatch();
        }
        data=modbus.readHoldingRegistersTest(1,120,19);
        for (int i=0;i<19;i++) {
            stmt.setInt(1,data[i]);
            stmt.setInt(2,3);
            stmt.setInt(3,i+121);
            stmt.addBatch();
        }
        boolean[] data1=modbus.readDiscreteInputTest(1,6,94);
        for (int i=0;i<94;i++) {
            stmt.setInt(1,data1[i]?1:0);
            stmt.setInt(2,1);
            stmt.setInt(3,i+7);
            stmt.addBatch();
        }
        data1=modbus.readDiscreteInputTest(1,100,100);
        for (int i=0;i<100;i++) {
            stmt.setInt(1,data1[i]?1:0);
            stmt.setInt(2,1);
            stmt.setInt(3,i+101);
            stmt.addBatch();
        }
        data1=modbus.readDiscreteInputTest(1,200,38);
        for (int i=0;i<38;i++) {
            stmt.setInt(1,data1[i]?1:0);
            stmt.setInt(2,1);
            stmt.setInt(3,i+201);
            stmt.addBatch();
        }
        stmt.executeBatch();
        con.commit();
        stmt.close();
    }
    @Override
    public void run() {
        try {
            long start=System.currentTimeMillis();
            updateValue();
            System.out.println("One time:"+(System.currentTimeMillis()-start));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
