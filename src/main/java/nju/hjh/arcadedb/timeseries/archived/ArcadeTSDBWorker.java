package nju.hjh.arcadedb.timeseries.archived;

import nju.hjh.utils.log.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

public class ArcadeTSDBWorker implements Runnable {
    public Socket socket;
    public final Logger logger;
    public final MessageHandler handler;

    public ArcadeTSDBWorker(Socket socket, Logger serverLogger) {
        this.socket = socket;
        logger = serverLogger.clone();
        logger.appendTag("remoteAddr", () -> String.format("remote: %s:%d", socket.getInetAddress(), socket.getPort()));
        handler = new MessageHandler(logger);
    }

    @Override
    public void run() {
        logger.logOnStdout("worker handling socket connection");
        PrintWriter writer;
        BufferedReader reader;
        try {
            writer = new PrintWriter(socket.getOutputStream());
            reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        } catch (IOException e) {
            logger.logOnStderr("failed to create io from socket");
            throw new RuntimeException(e);
        }

        String msg;
        try {
            while (true) {
                msg = reader.readLine();
                if (msg == null) {
                    Thread.sleep(1);
                } else {
                    //logger.logOnStdout("msg received: %s", (msg.length() > ServerUtils.SHOW_MESSAGE_LENGTH ? msg.substring(0, ServerUtils.SHOW_MESSAGE_LENGTH) + " ...(total " + msg.length() + " characters)" : msg));
                    if (msg.equals(ServerUtils.CONNECTION_CLOSE))
                        break;

                    writer.write(handler.handleMessage(msg).toJSONString() + "\n");
                    writer.flush();
                }
            }
        } catch (IOException e) {
            logger.logOnStderr("failed to read msg from client");
        } catch (InterruptedException e) {
            logger.logOnStderr("thread failed when waiting msg");
        }
        writer.write("close\n");
        writer.flush();

        try {
            reader.close();
            writer.close();
            socket.close();
        } catch (IOException ignored) {
        }

        logger.logOnStdout("connection closed");
    }
}
