package nju.hjh.arcadedb.timeseries.server;

import nju.hjh.arcadedb.timeseries.server.utils.DatabaseUtils;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class ArcadeTimeseriesServerApplication {
    public static void main(String[] args) {
        SpringApplication.run(ArcadeTimeseriesServerApplication.class, args);
        Runtime.getRuntime().addShutdownHook(new Thread(DatabaseUtils::closeAllDatabase));
    }
}