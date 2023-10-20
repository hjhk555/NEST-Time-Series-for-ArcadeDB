import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.graph.Vertex;
import nju.hjh.utils.exception.ExceptionSerializer;
import nju.hjh.utils.log.Logger;
import nju.hjh.arcadedb.timeseries.DataPointSet;
import nju.hjh.arcadedb.timeseries.DataType;
import nju.hjh.arcadedb.timeseries.UpdateStrategy;
import nju.hjh.arcadedb.timeseries.TimeseriesEngine;
import nju.hjh.arcadedb.timeseries.datapoint.StringDataPoint;
import nju.hjh.arcadedb.timeseries.exception.TimeseriesException;
import nju.hjh.arcadedb.timeseries.statistics.UnfixedStatistics;

import java.util.ArrayList;
import java.util.Random;

public class TimeseriesUnfixedTest {
    /**
     * from stack overflow
     * <a href=https://stackoverflow.com/questions/2863852/how-to-generate-a-random-string-in-java>How to generate a random String in Java</a>
     */
    public static String generateString(Random rng, String characters, int length)
    {
        char[] text = new char[length];
        for (int i = 0; i < length; i++)
        {
            text[i] = characters.charAt(rng.nextInt(characters.length()));
        }
        return new String(text);
    }

    public static void main(String[] args) {
        Logger logger = Logger.getPureLogger("TSUnfixed");
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

            final int testSize = 12345678;
            final int commitSize = 1000000;
            final String charUsed = "abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
            final int strLen = 10;

            ArrayList<String> strList = new ArrayList<>();

            Random ran = new Random();

            for (int i=0; i<testSize; i++){
                strList.add(generateString(ran, charUsed, strLen));
            }

            long periodStartTime = System.currentTimeMillis();

            for (int i=0; i<testSize; i++){
                int index = ran.nextInt(testSize);
                if (i > 0 && i % commitSize == 0) {
                    tsEngine.commit();

                    long periodElapsed = System.currentTimeMillis() - periodStartTime;
                    periodStartTime = System.currentTimeMillis();
                    logger.logOnStdout("inserted %d datapoints using %d ms", commitSize, periodElapsed);

                    tsEngine.begin();
                }
                tsEngine.insertDataPoint(testVertex.modify(), "status", DataType.STRING, new StringDataPoint(index, strList.get(index)), UpdateStrategy.UPDATE);
            }

            tsEngine.commit();

            long elapsed = System.currentTimeMillis() - startTime;
            logger.logOnStdout("insert "+testSize+" datapoints into status of testVertex using "+elapsed+" ms");

            tsEngine.begin();

            for (int i=0; i<20; i++){
                int queryStart = ran.nextInt(testSize);
                int queryEnd = ran.nextInt(queryStart, testSize);

                startTime = System.currentTimeMillis();
                UnfixedStatistics statistics = (UnfixedStatistics) tsEngine.aggregativeQuery(testVertex, "status", queryStart, queryEnd);

                DataPointSet firstRes = tsEngine.periodQuery(testVertex, "status", statistics.firstTime, statistics.firstTime);
                String firstValue = (String) firstRes.next().getValue();

                DataPointSet lastRes = tsEngine.periodQuery(testVertex, "status", statistics.lastTime, statistics.lastTime);
                String lastValue = (String) lastRes.next().getValue();

                elapsed = System.currentTimeMillis() - startTime;
                logger.logOnStdout("query [%d, %d] get %s with firstValue=%s, lastValue=%s in %d ms with correct=%s",
                        queryStart, queryEnd, statistics.toPrettyPrintString(), firstValue, lastValue, elapsed,
                        (strList.get((int) statistics.firstTime).equals(firstValue) && strList.get((int) statistics.lastTime).equals(lastValue)));
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
