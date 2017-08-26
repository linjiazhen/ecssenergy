package com.wintoo.tools;

import com.serotonin.io.serial.*;
import com.serotonin.modbus4j.*;
import com.serotonin.modbus4j.exception.*;
import com.serotonin.modbus4j.msg.*;

import java.util.Arrays;

public class ModBus{
    private ModbusMaster master;

    public  ModBus(String portId,int dataBits,int baudRate)  {
        SerialParameters serialParameters = new SerialParameters();

        //设定MODBUS通讯的串行口
        serialParameters.setCommPortId(portId);

        //设定成无奇偶校验
        serialParameters.setParity(0);

        //设定成数据位是8位
        serialParameters.setDataBits(dataBits);

        //设定为1个停止位
        serialParameters.setStopBits(1);

        serialParameters.setPortOwnerName("Wintoo");

        //串行口上的波特率
        serialParameters.setBaudRate(baudRate);

        ModbusFactory modbusFactory = new ModbusFactory();
        master = modbusFactory.createRtuMaster(serialParameters);
        try {
            master.init();
           //readDiscreteInputTest(master,1,189,8);
           // writeRegistersTest(master,SLAVE_ADDRESS, 0, new short[]{0x31,0xb,0xc,0xd,0xe,0x9, 0x8, 0x7, 0x6} );
            //readHoldingRegistersTest(master,1,0,8);
        } catch (Exception e){
            e.printStackTrace();
            master.destroy();
        }
    }
    public void destroy(){
        master.destroy();
    }
    /**
     * 读开关量型的输入信号
     * @param slaveId 从站地址
     * @param start 起始偏移量
     * @param len 待读的开关量的个数
     */
    public boolean[] readDiscreteInputTest(int slaveId, int start, int len) {
        try {
            ReadDiscreteInputsRequest request = new ReadDiscreteInputsRequest(slaveId, start, len);
            ReadDiscreteInputsResponse response = (ReadDiscreteInputsResponse) master.send(request);

            if (response.isException())
                System.out.println("Exception response: message=" + response.getExceptionMessage());
            else
                System.out.println(Arrays.toString(response.getBooleanData()));
            return response.getBooleanData();
        }
        catch (ModbusTransportException e) {
            e.printStackTrace();
        }
        return new boolean[0];
    }

    /**
     * 读保持寄存器上的内容
     * @param slaveId 从站地址
     * @param start 起始地址的偏移量
     * @param len 待读寄存器的个数
     */
    public short[] readHoldingRegistersTest(int slaveId, int start, int len) {
        try {
            ReadHoldingRegistersRequest request = new ReadHoldingRegistersRequest(slaveId, start, len);
            ReadHoldingRegistersResponse response = (ReadHoldingRegistersResponse) master.send(request);

            if (response.isException())
                System.out.println("Exception response: message=" + response.getExceptionMessage());
            else
                System.out.println(Arrays.toString(response.getShortData()));
            return response.getShortData();
        }
        catch (ModbusTransportException e) {
            e.printStackTrace();
        }
        return new short[0];
    }

    /**
     * 批量写数据到保持寄存器
     * @param slaveId 从站地址
     * @param start 起始地址的偏移量
     * @param values 待写数据
     */
    public void writeRegistersTest(int slaveId, int start, short[] values) {
        try {
            WriteRegistersRequest request = new WriteRegistersRequest(slaveId, start, values);
            WriteRegistersResponse response = (WriteRegistersResponse) master.send(request);

            if (response.isException())
                System.out.println("Exception response: message=" + response.getExceptionMessage());
            else
                System.out.println("Success");
        }
        catch (ModbusTransportException e) {
            e.printStackTrace();
        }
    }
}

