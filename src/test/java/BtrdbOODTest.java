import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.graph.Vertex;
import nju.hjh.arcadedb.timeseries.btrdb.BtrdbEngine;
import nju.hjh.arcadedb.timeseries.btrdb.DoubleStatistics;
import nju.hjh.arcadedb.timeseries.exception.TimeseriesException;
import nju.hjh.utils.exception.ExceptionSerializer;
import nju.hjh.utils.log.Logger;

import java.util.LinkedList;
import java.util.Queue;
import java.util.Random;

public class BtrdbOODTest {
    public static void main(String[] args) {
        Logger logger = Logger.getPureLogger("BTRDB_OOD");
        DatabaseFactory dbf = new DatabaseFactory("./databases/btrdbTest");

        Database database;
        if (dbf.exists()) {
            database = dbf.open();
        } else {
            database = dbf.create();
        }

        database.begin();
        if (!database.getSchema().existsType("test")) {
            database.getSchema().createVertexType("test");
        }
        Vertex testVertex = database.newVertex("test").save();
        database.commit();

        logger.logOnStdout("created vertex rid is " + testVertex.getIdentity());
        BtrdbEngine tsEngine = new BtrdbEngine(database);

        tsEngine.begin();
        try {
            long startTime = System.currentTimeMillis();

            final int testSize = 12345678;
            final int commitSize = 1000000;
            final int oodProb = 10;

            Random ran = new Random();

            Queue<Integer> oodData = new LinkedList<>();
            long count = 0;
            long periodStartTime = System.currentTimeMillis();

            for (int i = 0; i < testSize; i++) {
                if (ran.nextInt(100) < oodProb) {
                    oodData.offer(i);
                } else {
                    tsEngine.insertDataPoint(testVertex.modify(), "status", i, i);
                    count++;
                }
                if (count == commitSize) {
                    tsEngine.commit();

                    long periodElapsed = System.currentTimeMillis() - periodStartTime;
                    periodStartTime = System.currentTimeMillis();
                    logger.logOnStdout("inserted %d datapoints using %d ms", count, periodElapsed);

                    count = 0;
                    tsEngine.begin();
                }
            }

            logger.logOnStdout("start to insert out-of-order datapoint");

            long oodSize = oodData.size();
            while (!oodData.isEmpty()) {
                int ood = oodData.poll();
                tsEngine.insertDataPoint(testVertex.modify(), "status", ood, ood);
                count++;

                if (count == commitSize) {
                    tsEngine.commit();

                    long periodElapsed = System.currentTimeMillis() - periodStartTime;
                    periodStartTime = System.currentTimeMillis();
                    logger.logOnStdout("inserted %d datapoints using %d ms", count, periodElapsed);

                    count = 0;
                    tsEngine.begin();
                }
            }

            tsEngine.commit();

            long elapsed = System.currentTimeMillis() - startTime;
            logger.logOnStdout("inserted %d datapoints including %d out-of-order ones using %d ms", testSize, oodSize, elapsed);

            tsEngine.begin();
            for (int i = 0; i < 20; i++) {
                int queryStart = ran.nextInt(testSize);
                int queryEnd = ran.nextInt(queryStart, testSize);
                long ans = (long) (queryEnd + queryStart) * (queryEnd - queryStart + 1) / 2;

                startTime = System.currentTimeMillis();

                DoubleStatistics statistics = tsEngine.aggregativeQuery(testVertex, "status", queryStart, queryEnd);
                double sum = statistics.sum;
                elapsed = System.currentTimeMillis() - startTime;
                logger.logOnStdout("query [%d, %d] get %s in %d ms with correctSum=%d", queryStart, queryEnd, statistics.toPrettyPrintString(), elapsed, ans);
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
