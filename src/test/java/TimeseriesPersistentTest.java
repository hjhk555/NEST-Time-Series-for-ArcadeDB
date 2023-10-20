import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.RID;
import com.arcadedb.graph.Vertex;
import nju.hjh.utils.exception.ExceptionSerializer;
import nju.hjh.utils.log.Logger;
import nju.hjh.arcadedb.timeseries.*;
import nju.hjh.arcadedb.timeseries.datapoint.DataPoint;
import nju.hjh.arcadedb.timeseries.datapoint.LongDataPoint;
import nju.hjh.arcadedb.timeseries.exception.TimeseriesException;
import nju.hjh.arcadedb.timeseries.statistics.LongStatistics;

import java.util.Random;

public class TimeseriesPersistentTest {
    public static void main(String[] args) {
        Logger logger = Logger.getPureLogger("TSPersist");
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
        database.commit();


        Vertex testVertex = database.lookupByRID(new RID(database, 1, 14), false).asVertex();
        logger.logOnStdout("tested vertex rid is "+testVertex.getIdentity());
        TimeseriesEngine tsEngine = new TimeseriesEngine(database);

        tsEngine.begin();
        try {
            int testSize = 1000000;

            Random ran = new Random();

            for (int i=0; i<20; i++){
                int queryStart = ran.nextInt(testSize);
                int queryEnd = ran.nextInt(queryStart, testSize);
                queryStart = 722516;
                queryEnd = 913191;
                long ans = (long) (queryEnd + queryStart) * (queryEnd - queryStart + 1);

                long startTime = System.currentTimeMillis();

                LongStatistics statistics = (LongStatistics) tsEngine.aggregativeQuery(testVertex, "status", queryStart, queryEnd);
                long sum = statistics.sum;
                long elapsed = System.currentTimeMillis() - startTime;
                logger.logOnStdout("query [%d, %d] get %s in %d ms with correctSum=%d, correct=%s", queryStart, queryEnd, statistics.toPrettyPrintString(), elapsed, ans, sum == ans);
            }

            for (int i=0; i<20; i++){
                int queryStart = ran.nextInt(testSize);
                int queryEnd = ran.nextInt(queryStart, testSize);
                logger.logOnStdout("querying [%d, %d]:", queryStart, queryEnd);
                long startTime = System.currentTimeMillis();

                DataPointSet rs = tsEngine.periodQuery(testVertex, "status", queryStart, queryEnd);
                DataPoint dp;
                int cur = queryStart;
                while ((dp = rs.next()) != null){
                    if (dp instanceof LongDataPoint longDP){
                        if (longDP.value != cur*2)
                            logger.logOnStderr("result not match at %d", cur);
                    }
                    cur++;
                }

                cur--;
                if (cur != queryEnd)
                    logger.logOnStderr("result should end at %d but end at %d", queryEnd, cur);

                long elapsed = System.currentTimeMillis() - startTime;
                logger.logOnStdout("query [%d, %d] finished in %d ms", queryStart, queryEnd, elapsed);
            }

        } catch (TimeseriesException e) {
            logger.logOnStderr(ExceptionSerializer.serializeAll(e));
            database.rollback();
            database.close();
            return;
        }
        tsEngine.commit();

        database.close();
    }
}
