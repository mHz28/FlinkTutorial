package com.windowing;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.ServerSocket;
import java.sql.Timestamp;

public class SlidingWindowDataServerEventTime {
    public static void main(String[] args) throws IOException{
        ServerSocket listener = new ServerSocket(9090);
        try{
            Socket socket = listener.accept();
            System.out.println("Got new connection: " + socket.toString());
            try {
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                Random rand = new Random();
                Date d = new Date();
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS", Locale.US);

                GregorianCalendar calendar = new GregorianCalendar(TimeZone.getTimeZone("US/Central"));
                calendar.setTimeInMillis(System.currentTimeMillis());

//                System.out.println("GregorianCalendar -"+sdf.format(calendar.getTime()));
                while (true){
                    int i = rand.nextInt(100);
                    String s = "" + System.currentTimeMillis() + "," + i;
                    System.out.println(s);
                    /* <timestamp>,<random-number> */
                    out.println(s);
                    Thread.sleep(50);
                }

            } finally{
                socket.close();
            }

        } catch(Exception e ){
            e.printStackTrace();
        } finally{

            listener.close();
        }
    }
}

