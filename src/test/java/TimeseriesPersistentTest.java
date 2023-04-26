import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.RID;
import com.arcadedb.graph.Vertex;
import com.arcadedb.timeseries.*;
import indi.hjhk.log.Logger;

import java.util.Random;

public class TimeseriesPersistentTest {
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
        database.commit();


        Vertex testVertex = database.lookupByRID(new RID(database, 1, 1), false).asVertex();
        Logger.logOnStdout("tested vertex rid is "+testVertex.getIdentity());
        TimeseriesEngine tsEngine = new TimeseriesEngine(database);

        tsEngine.begin();
        try {
            int testSize = 123456789;

            Random ran = new Random();

            for (int i=0; i<20; i++){
                int queryStart = ran.nextInt(testSize);
                int queryEnd = ran.nextInt(queryStart, testSize);
                long ans = (long) (queryEnd + queryStart) * (queryEnd - queryStart + 1) / 2;

                long startTime = System.currentTimeMillis();

                LongStatistics statistics = (LongStatistics) tsEngine.aggregativeQuery(testVertex, "status", queryStart, queryEnd);
                long sum = statistics.sum;
                long elapsed = System.currentTimeMillis() - startTime;
                Logger.logOnStdout("query [%d, %d] get %s in %d ms with correctSum=%d, correct=%s", queryStart, queryEnd, statistics.toPrettyPrintString(), elapsed, ans, sum == ans);
            }

            for (int i=0; i<20; i++){
                int queryStart = ran.nextInt(testSize);
                int queryEnd = ran.nextInt(queryStart, testSize);
                Logger.logOnStdout("querying [%d, %d]:", queryStart, queryEnd);
                long startTime = System.currentTimeMillis();

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

                long elapsed = System.currentTimeMillis() - startTime;
                Logger.logOnStdout("query [%d, %d] finished in %d ms", queryStart, queryEnd, elapsed);
            }

        } catch (TimeseriesException e) {
            e.printStackTrace();
            database.rollback();
            database.close();
            return;
        }
        tsEngine.commit();

        database.close();
    }
}
