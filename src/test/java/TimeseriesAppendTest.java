import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.graph.Vertex;
import nju.hjh.arcadedb.timeseries.DataPointList;
import nju.hjh.arcadedb.timeseries.types.DataType;
import nju.hjh.arcadedb.timeseries.UpdateStrategy;
import nju.hjh.arcadedb.timeseries.NestEngine;
import nju.hjh.arcadedb.timeseries.datapoint.StringDataPoint;
import nju.hjh.arcadedb.timeseries.exception.TimeseriesException;
import nju.hjh.arcadedb.timeseries.statistics.Statistics;
import nju.hjh.utils.exception.ExceptionSerializer;
import nju.hjh.utils.log.Logger;

import java.util.ArrayList;
import java.util.Random;

public class TimeseriesAppendTest {
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
        Logger logger = Logger.getPureLogger("TSString");
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
        NestEngine tsEngine = new NestEngine(database);

        tsEngine.begin();
        try {
            long startTime = System.currentTimeMillis();

            final int testSize = 123456;
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
                if (i > 0 && i % commitSize == 0) {
                    tsEngine.commit();

                    long periodElapsed = System.currentTimeMillis() - periodStartTime;
                    periodStartTime = System.currentTimeMillis();
                    logger.logOnStdout("inserted datapoints range=[%d, %d) using %d ms", i-commitSize , i, periodElapsed);

                    tsEngine.begin();
                }
                tsEngine.insertDataPoint(testVertex.modify(), "status", i/10, strList.get(i), UpdateStrategy.APPEND);
            }

            tsEngine.commit();

            long elapsed = System.currentTimeMillis() - startTime;
            logger.logOnStdout("insert "+testSize+" datapoints into status of testVertex using "+elapsed+" ms");

            tsEngine.begin();

            for (int i=0; i<20; i++){
                int queryStart = ran.nextInt(testSize/10);
                int queryEnd = ran.nextInt(queryStart, testSize/10);

                startTime = System.currentTimeMillis();
                Statistics statistics = tsEngine.aggregativeQuery(testVertex, "status", queryStart, queryEnd);
                DataPointList fset = tsEngine.periodQuery(testVertex, "status", statistics.firstTime, statistics.firstTime, 1);
                String strFirst = fset.next().getValue().toString();
                DataPointList lset = tsEngine.periodQuery(testVertex, "status", statistics.lastTime, statistics.lastTime, 1);
                String strLast = lset.next().getValue().toString();

                elapsed = System.currentTimeMillis() - startTime;
                logger.logOnStdout("query [%d, %d] get %s in %d ms with realfirst=%s, reallast=%s, first=%s, last=%s",
                        queryStart, queryEnd, statistics.toPrettyPrintString(), elapsed,
                        strList.get((int) statistics.firstTime*10), strList.get((int) statistics.lastTime*10), strFirst, strLast);
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
