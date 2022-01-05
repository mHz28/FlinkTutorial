package com.windowing;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.ServerSocket;

public class TumblingWindowsDataServer
{
    public static void main(String[] args) throws IOException
    {
        ServerSocket listener = new ServerSocket(9090);
        try{
            Socket socket = listener.accept();
            System.out.println("Got new connection: " + socket.toString());
            String dir = "/Users/ekwong/Downloads/flink_sandbox/windows/tumbling/flink_tumbling_windows_with_processing_time/";

            BufferedReader br = new BufferedReader(new FileReader(dir + "avg"));

            try {
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                String line;
                while ((line = br.readLine()) != null){

                    out.println(line);
                    System.out.println("sending: " + line);
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

