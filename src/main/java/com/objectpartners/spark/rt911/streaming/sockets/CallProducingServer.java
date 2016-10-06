package com.objectpartners.spark.rt911.streaming.sockets;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.zip.GZIPInputStream;

@Component
class CallProducingServer implements Runnable {
    private static Logger logger = LoggerFactory.getLogger(CallProducingServer.class);

    private final int port;
    private final String dataFileName;

    CallProducingServer(int port, String dataFileName) {
        this.port = port;
        this.dataFileName = dataFileName;
    }


    @Override
    public void run() {
        logger.info("call producing client is running ....");

        try {
            final InputStream is = CallProducingServer.class.getResourceAsStream("/"+dataFileName);
            final BufferedInputStream bis =  new BufferedInputStream(is);
            final GZIPInputStream iis = new GZIPInputStream(bis);
            final InputStreamReader gzipReader = new InputStreamReader(iis);
            final BufferedReader br = new BufferedReader(gzipReader);
            br.readLine(); // skip header or first line

            final ServerSocket server = new ServerSocket(port);

            String line;
            while((line = br.readLine()) != null) {
                Socket socket = server.accept();
                OutputStream output = socket.getOutputStream();
                String data = line + "\n";
                output.write(data.getBytes());
                output.flush();
                socket.close();
            }
            logger.info("all input processed");
            br.close();
            iis.close();
            server.close();
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }
}
