import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.graph.Vertex;
import nju.hjh.arcadedb.timeseries.btrdb.BtrdbEngine;
import nju.hjh.arcadedb.timeseries.btrdb.DoubleStatistics;
import nju.hjh.arcadedb.timeseries.exception.TimeseriesException;
import nju.hjh.utils.exception.ExceptionSerializer;
import nju.hjh.utils.log.Logger;

import java.util.Random;

public class BtrdbInsertTest {
    public static void main(String[] args) {
        Logger logger = Logger.getPureLogger("btrdb_insert");
        DatabaseFactory dbf = new DatabaseFactory("./databases/btrdbTest");

        Database database;
        if (dbf.exists()){
            database = dbf.open();
        }else{
            database = dbf.create();
        }

        database.begin();
        if (!database.getSchema().existsType("test")){
            database.getSchema().createVertexType("test");
        }
        Vertex testVertex = database.newVertex("test").save();
        database.commit();

        logger.logOnStdout("created vertex rid is "+testVertex.getIdentity());
        BtrdbEngine tsEngine = new BtrdbEngine(database);

        tsEngine.begin();
        try {
            long startTime = System.currentTimeMillis();

            final int testSize = 123456789;
            final int commitSize = 1000000;

            Random ran = new Random();

            //long periodStartTime = System.currentTimeMillis();

            for (int i=0; i<testSize; i++){
                if (i > 0 && i % commitSize == 0) {
                    tsEngine.commit();

                    //long periodElapsed = System.currentTimeMillis() - periodStartTime;
                    //periodStartTime = System.currentTimeMillis();
                    //logger.logOnStdout("inserted datapoints range=[%d, %d) using %d ms", i-commitSize , i, periodElapsed);

                    tsEngine.begin();
                }
                tsEngine.insertDataPoint(testVertex.modify(), "status", i, i);
            }

            tsEngine.commit();

            long elapsed = System.currentTimeMillis() - startTime;
            logger.logOnStdout("insert "+testSize+" datapoints into status of testVertex using "+elapsed+" ms");

            tsEngine.begin();

            for (int i=0; i<20; i++){
                int queryStart = ran.nextInt(testSize);
                int queryEnd = ran.nextInt(queryStart, testSize);
                long ans = (long) (queryEnd + queryStart) * (queryEnd - queryStart + 1) / 2;

                startTime = System.currentTimeMillis();

                DoubleStatistics statistics = tsEngine.aggregativeQuery(testVertex, "status", queryStart, queryEnd);
                double sum = statistics.sum;
                elapsed = System.currentTimeMillis() - startTime;
                logger.logOnStdout("query [%d, %d] get %s in %d ms with correctSum=%d", queryStart, queryEnd, statistics.toPrettyPrintString(), elapsed, ans, sum == ans);
            }

            tsEngine.commit();
        } catch (TimeseriesException e) {
            logger.logOnStderr(ExceptionSerializer.serializeAll(e));
            tsEngine.rollback();
            database.close();
            return;
        }

        database.close();
    }
}
