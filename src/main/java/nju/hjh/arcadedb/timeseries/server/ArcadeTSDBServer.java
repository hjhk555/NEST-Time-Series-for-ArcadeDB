package nju.hjh.arcadedb.timeseries.server;

import com.arcadedb.database.Database;
import nju.hjh.utils.log.Logger;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class ArcadeTSDBServer {
    public static void main(String[] args) {
        Logger logger = Logger.getPureLogger("TSDBServer");
        logger.logSilent("ArcadeTSDB server log");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            for (Database database : ArcadedbUtils.databaseInstances.values()){
                database.close();
            }
            logger.logOnStdout("server shutdown.");
        }));

        ServerSocket sSocket;
        try {
            sSocket = new ServerSocket(ServerUtils.DEFAULT_SERVER_PORT);
        } catch (IOException e) {
            logger.logOnStderr("failed to start server socket at port %d", ServerUtils.DEFAULT_SERVER_PORT);
            throw new RuntimeException(e);
        }
        logger.logOnStdout("server socket start at port %d", ServerUtils.DEFAULT_SERVER_PORT);

        while (true){
            Socket newSocket = null;
            try {
                newSocket = sSocket.accept();
            } catch (IOException e) {
                logger.logOnStderr("failed to accept new socket");
            }
            if (newSocket != null){
                ArcadeTSDBWorker newWorker = new ArcadeTSDBWorker(newSocket, logger);
                Thread newThread = new Thread(newWorker);
                newThread.start();
            }
        }
    }
}
