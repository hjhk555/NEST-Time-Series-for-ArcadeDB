import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.graph.Vertex;
import com.arcadedb.timeseries.*;
import indi.hjhk.log.Logger;

import java.util.Random;

public class TimeseriesPeriodQuertTest {
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

            final int testSize = 10000000;
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
                tsEngine.insertDataPoint(testVertex, "status", new DataType(DataType.BaseType.LONG, 0), new LongDataPoint(i, i));
            }

            tsEngine.commit();

            long elapsed = System.currentTimeMillis() - startTime;
            Logger.logOnStdout("insert "+testSize+" datapoints into status of testVertex using "+elapsed+" ms");

            tsEngine.begin();

            for (int i=0; i<20; i++){
                int queryStart = ran.nextInt(testSize);
                int queryEnd = ran.nextInt(queryStart, testSize);
                Logger.logOnStdout("querying [%d, %d]:", queryStart, queryEnd);
                startTime = System.currentTimeMillis();

                DataPointSet rs = tsEngine.periodQuery(testVertex, "status", queryStart, queryEnd);
                DataPoint dp;
                int cur = queryStart;
                while ((dp = rs.next()) != null){
                    if (dp instanceof LongDataPoint longDP){
                        if (longDP.value != cur)
                            Logger.logOnStderr("result not match at %d", cur);
                    }
                    cur++;
                }

                cur--;
                if (cur != queryEnd)
                    Logger.logOnStderr("result should end at %d but end at %d", queryEnd, cur);

                elapsed = System.currentTimeMillis() - startTime;
                Logger.logOnStdout("query [%d, %d] finished in %d ms", queryStart, queryEnd, elapsed);
            }

        } catch (TimeseriesException e) {
            e.printStackTrace();
            tsEngine.rollback();
            database.close();
            return;
        }
        tsEngine.commit();

        database.close();
    }
}
