import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.graph.Vertex;
import indi.hjhk.exception.ExceptionSerializer;
import indi.hjhk.log.Logger;
import nju.hjh.arcadedb.timeseries.*;
import nju.hjh.arcadedb.timeseries.datapoint.DataPoint;
import nju.hjh.arcadedb.timeseries.datapoint.LongDataPoint;
import nju.hjh.arcadedb.timeseries.exception.TimeseriesException;

import java.util.Random;

public class TimeseriesPeriodQuertTest {
    public static void main(String[] args) {
        Logger logger = Logger.getPureLogger("TSPeriod");
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

        logger.logOnStdout("created vertex rid is "+testVertex.getIdentity());
        TimeseriesEngine tsEngine = new TimeseriesEngine(database);

        tsEngine.begin();
        try {
            long startTime = System.currentTimeMillis();

            final int testSize = 10000000;
            final int commitSize = 1000000;

            Random ran = new Random();

            long periodStartTime = System.currentTimeMillis();

            for (int i=0; i<testSize; i++){
                if (i > 0 && i % commitSize == 0) {
                    tsEngine.commit();

                    long periodElapsed = System.currentTimeMillis() - periodStartTime;
                    periodStartTime = System.currentTimeMillis();
                    logger.logOnStdout("inserted datapoints range=[%d, %d) using %d ms", i-commitSize , i, periodElapsed);

                    tsEngine.begin();
                }
                tsEngine.insertDataPoint(testVertex, "status", DataType.LONG, new LongDataPoint(i, i), TSUpdateStrategy.ERROR);
            }

            tsEngine.commit();

            long elapsed = System.currentTimeMillis() - startTime;
            logger.logOnStdout("insert "+testSize+" datapoints into status of testVertex using "+elapsed+" ms");

            tsEngine.begin();

            for (int i=0; i<20; i++){
                int queryStart = ran.nextInt(testSize);
                int queryEnd = ran.nextInt(queryStart, testSize);
                logger.logOnStdout("querying [%d, %d]:", queryStart, queryEnd);
                startTime = System.currentTimeMillis();

                DataPointSet rs = tsEngine.periodQuery(testVertex, "status", queryStart, queryEnd);
                DataPoint dp;
                int cur = queryStart;
                while ((dp = rs.next()) != null){
                    if (dp instanceof LongDataPoint longDP){
                        if (longDP.value != cur)
                            logger.logOnStderr("result not match at %d", cur);
                    }
                    cur++;
                }

                cur--;
                if (cur != queryEnd)
                    logger.logOnStderr("result should end at %d but end at %d", queryEnd, cur);

                elapsed = System.currentTimeMillis() - startTime;
                logger.logOnStdout("query [%d, %d] finished in %d ms", queryStart, queryEnd, elapsed);
            }

        } catch (TimeseriesException e) {
            logger.logOnStderr(ExceptionSerializer.serializeAll(e));
            tsEngine.rollback();
            database.close();
            return;
        }
        tsEngine.commit();

        database.close();
    }
}
