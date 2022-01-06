package com.state;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Date;
import java.util.Random;

public class ValueStateDataServer {

    public static void main(String[] args) throws IOException {
        ServerSocket listener = new ServerSocket(9090);
        try{
            Socket socket = listener.accept();
            System.out.println("Got new connection: " + socket.toString());
            try {
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                Random rand = new Random();
                Date d = new Date();

                int count = 0;
                int tenSum = 0;

                for (int x = 0; x<50; ++x){

                    int key = (x%2) + 1;
                    String s = key + "," + x;
                    System.out.println(s);
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


