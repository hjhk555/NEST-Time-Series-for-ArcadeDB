package nju.hjh.arcadedb.timeseries.client;

import com.alibaba.fastjson2.JSONObject;
import nju.hjh.arcadedb.timeseries.client.gui.MainGUI;
import nju.hjh.arcadedb.timeseries.server.ServerUtils;
import nju.hjh.utils.exception.ExceptionSerializer;
import nju.hjh.utils.log.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

public class ArcadeTSDBClient{
    final String hostIP;
    final int hostPort;
    final Logger logger = Logger.getPureLogger("TSDBClient");;
    Socket socket = null;
    PrintWriter writer = null;
    BufferedReader reader = null;

    public ArcadeTSDBClient(){
        this(ClientUtils.DEFAULT_SERVER_HOST, ClientUtils.DEFAULT_SERVER_PORT);
    }

    public ArcadeTSDBClient(String hostIP){
        this(hostIP, ClientUtils.DEFAULT_SERVER_PORT);
    }

    public ArcadeTSDBClient(String hostIP, int hostPort) {
        this.hostIP = hostIP;
        this.hostPort = hostPort;

        logger.logSilent("ArcadeTSDB client log");
    }

    public void connect(){
        if (socket == null || socket.isClosed()) {
            try {
                socket = new Socket(hostIP, hostPort);
                writer = new PrintWriter(socket.getOutputStream());
                reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                logger.logOnStdout("connected to server at %s:%d", hostIP, hostPort);
            } catch (IOException e) {
                logger.logOnStderr("failed to connect to server at %s:%d", hostIP, hostPort);
                logger.logOnStderr(ExceptionSerializer.serializeAll(e));
                close();
            }
        }
    }

    public boolean isConnected(){
        if (socket == null) return false;
        return socket.isConnected();
    }

    public void close(){
        if (socket == null) return;
        sendClose();
        try {
            if (reader != null)
                reader.close();
            if (writer != null)
                writer.close();
            socket.close();
            socket = null;
        }catch (IOException ignored){}
        logger.logOnStdout("connection to %s:%d closed", hostIP, hostPort);
    }

    public void sendClose(){
        if (writer == null) return;
        writer.write(ServerUtils.CONNECTION_CLOSE +"\n");
        writer.flush();
    }

    /**
     * send json message to server, return until result received
     * @param json json object to send
     * @return result from server, null if failed
     */
    public String sendJsonAndWaitResult(JSONObject json){
        if (socket == null || socket.isClosed())
            return null;

        String msg = json.toJSONString();
        logger.logSilent("to server: %s", (msg.length() > ClientUtils.SHOW_MESSAGE_LENGTH ? msg.substring(0, ClientUtils.SHOW_MESSAGE_LENGTH) + " ...(total " + msg.length() + " characters)" : msg));
        writer.write(msg + "\n");
        writer.flush();

        String ret;
        try {
            ret = reader.readLine();
        } catch (IOException e) {
            logger.logOnStderr("failed to read response from server");
            return null;
        }

        if (ret != null) {
            logger.logSilent("from server: %s", (ret.length() > ClientUtils.SHOW_MESSAGE_LENGTH ? ret.substring(0, ClientUtils.SHOW_MESSAGE_LENGTH) + " ...(total " + ret.length() + " characters)" : ret));
        } else {
            logger.logOnStderr("socket closed from server");
        }

        return ret;
    }

    public String getHost(){
        return hostIP + ":" + hostPort;
    }

    public static void main(String[] args) {
        MainGUI.main(args);
    }
}
