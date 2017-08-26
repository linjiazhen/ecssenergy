package com.wintoo.tools;


import gnu.io.*;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Enumeration;
import java.util.TooManyListenersException;

/**
 * Created by HaiziGe on 2015/10/16.
 */
public class SPComm implements SerialPortEventListener {

    private static CommPortIdentifier portId;
    private static Enumeration<CommPortIdentifier> portList;

    public static InputStream inputStream;
    public static OutputStream outputStream;

    public static SerialPort serialPort;

    public static void init() {
        portList = CommPortIdentifier.getPortIdentifiers();
        while (portList.hasMoreElements()) {
            portId = (CommPortIdentifier) portList.nextElement();
            if (portId.getPortType() == CommPortIdentifier.PORT_SERIAL){
                if (portId.getName().equals("COM3")) {
                    System.out.println("打开串口中..." + portId.getName().toString());
                    try {
                        serialPort = (SerialPort) portId.open("SerialPort-Test", 2000);
                        serialPort.addEventListener(new SPComm());
                        serialPort.notifyOnDataAvailable(true);

                        serialPort.setSerialPortParams(9600,SerialPort.DATABITS_8, SerialPort.STOPBITS_1,SerialPort.PARITY_NONE);
                        System.out.println("串口已打开!");
                        outputStream = serialPort.getOutputStream();
                        inputStream = serialPort.getInputStream();
                    } catch (PortInUseException e) {
                        e.printStackTrace();
                    } catch (TooManyListenersException e) {
                        e.printStackTrace();
                    } catch (UnsupportedCommOperationException e) {
                        e.printStackTrace();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    private static byte[] readBuffer = new byte[1024];
    int numBytes;
    public void serialEvent(SerialPortEvent event) {
        switch (event.getEventType()) {
            case SerialPortEvent.BI:
            case SerialPortEvent.OE:
            case SerialPortEvent.FE:
            case SerialPortEvent.PE:
            case SerialPortEvent.CD:
            case SerialPortEvent.CTS:
            case SerialPortEvent.DSR:
            case SerialPortEvent.RI:
            case SerialPortEvent.OUTPUT_BUFFER_EMPTY:
                break;
            case SerialPortEvent.DATA_AVAILABLE:
                try{
                    while (inputStream.available() > 0) {
                        numBytes = inputStream.read(readBuffer);
                    }
                    for(int i=0;i<numBytes;i++){
                        System.out.print((char)readBuffer[i]);
                    }
                }catch(IOException e){
                    e.printStackTrace();
                }
                break;
        }
    }

    public static String toUnicode(String str) {
        char[]arChar=str.toCharArray();
        int iValue=0;
        String uStr="";
        for(int i=0;i<arChar.length;i++) {
            iValue=(int)str.charAt(i);
            if(iValue<=256){
                uStr+="00"+Integer.toHexString(iValue);
            } else {
                uStr+=""+Integer.toHexString(iValue);
            }
        }
        return uStr;
    }

    public void sendMsg_init() throws Exception{
        String CSCS = "AT+CSCS=\"UCS2\"\r\n";
        String CSMP_UNICON = "AT+CSMP=17,167,0,25\r\n";
        String PHONE_NUM = "AT+CMGS=\"00310038003700350030003700310035003500370038\"\r\n";

        try {
            outputStream.write(CSCS.getBytes());
            Thread.sleep(1500);
            outputStream.write(CSMP_UNICON.getBytes());
            Thread.sleep(1500);
            outputStream.write(PHONE_NUM.getBytes());
            Thread.sleep(2000);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void sendMsg(String messageString) throws Exception{
        try {
            outputStream.write(toUnicode(messageString).getBytes());
            Thread.sleep(1500);
            outputStream.write(0x1a);
            Thread.sleep(5000);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
