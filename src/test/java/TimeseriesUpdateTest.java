import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.graph.Vertex;
import indi.hjhk.exception.ExceptionSerializer;
import indi.hjhk.log.Logger;
import nju.hjh.arcadedb.timeseries.DataType;
import nju.hjh.arcadedb.timeseries.TimeseriesEngine;
import nju.hjh.arcadedb.timeseries.datapoint.LongDataPoint;
import nju.hjh.arcadedb.timeseries.exception.TimeseriesException;
import nju.hjh.arcadedb.timeseries.statistics.LongStatistics;

import java.util.Random;

public class TimeseriesUpdateTest {
    public static void main(String[] args) {
        DatabaseFactory dbf = new DatabaseFactory("./databases/tsTest");

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

        Logger.logOnStdout("created vertex rid is "+testVertex.getIdentity());
        TimeseriesEngine tsEngine = new TimeseriesEngine(database);

        tsEngine.begin();
        try {
            long startTime = System.currentTimeMillis();

            final int testSize = 1000000;
            final int commitSize = 1000000;

            Random ran = new Random();

            long periodStartTime = System.currentTimeMillis();

            for (int i=0; i<testSize; i++){
                if (i > 0 && i % commitSize == 0) {
                    tsEngine.commit();

                    long periodElapsed = System.currentTimeMillis() - periodStartTime;
                    periodStartTime = System.currentTimeMillis();
                    Logger.logOnStdout("inserted datapoints range=[%d, %d) using %d ms", i-commitSize , i, periodElapsed);

                    tsEngine.begin();
                }
                tsEngine.insertDataPoint(testVertex, "status", new DataType(DataType.BaseType.LONG, 0), new LongDataPoint(i, i), false);
            }

            tsEngine.commit();

            long elapsed = System.currentTimeMillis() - startTime;
            Logger.logOnStdout("insert "+testSize+" datapoints into status of testVertex using "+elapsed+" ms");

            tsEngine.begin();
            startTime = System.currentTimeMillis();
            periodStartTime = System.currentTimeMillis();

            // update data to 2x with updateIfExist flag
            for (int i=0; i<testSize; i++){
                if (i > 0 && i % commitSize == 0) {
                    tsEngine.commit();

                    long periodElapsed = System.currentTimeMillis() - periodStartTime;
                    periodStartTime = System.currentTimeMillis();
                    Logger.logOnStdout("updated datapoints range=[%d, %d) using %d ms", i-commitSize , i, periodElapsed);

                    tsEngine.begin();
                }
                tsEngine.insertDataPoint(testVertex, "status", new DataType(DataType.BaseType.LONG, 0), new LongDataPoint(i, 2*i), true);
            }

            tsEngine.commit();

            elapsed = System.currentTimeMillis() - startTime;
            Logger.logOnStdout("update "+testSize+" datapoints into status of testVertex using "+elapsed+" ms");

            tsEngine.begin();

            for (int i=0; i<20; i++){
                int queryStart = ran.nextInt(testSize);
                int queryEnd = ran.nextInt(queryStart, testSize);
                queryStart = 120276;
                queryEnd = 425937;
                long ans = (long) (queryEnd + queryStart) * (queryEnd - queryStart + 1);

                startTime = System.currentTimeMillis();

                LongStatistics statistics = (LongStatistics) tsEngine.aggregativeQuery(testVertex, "status", queryStart, queryEnd);
                long sum = statistics.sum;
                elapsed = System.currentTimeMillis() - startTime;
                Logger.logOnStdout("query [%d, %d] get %s in %d ms with correctSum=%d, correct=%s", queryStart, queryEnd, statistics.toPrettyPrintString(), elapsed, ans, sum == ans);
            }
        } catch (TimeseriesException e) {
            Logger.logOnStderr(ExceptionSerializer.serializeAll(e));
            tsEngine.rollback();
            database.close();
            return;
        }
        tsEngine.commit();

        database.close();
    }
}
