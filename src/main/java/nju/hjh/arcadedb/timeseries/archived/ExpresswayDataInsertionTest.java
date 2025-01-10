import com.alibaba.fastjson2.JSONObject;
import nju.hjh.arcadedb.timeseries.client.ArcadeTSDBClient;
import nju.hjh.utils.log.Logger;
import sender.AdditionalSender;
import sender.Sender;
import simulator.Starter;
import tool.Config;

import java.io.IOException;
import java.text.SimpleDateFormat;

public class ExpresswayDataInsertionTest {
    public static String DB_NAME = "Expressway";
    public static String OBJ_TYPE = "gantry";
    public static SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

    public static void main(String[] args) throws IOException, InterruptedException {
        ArcadeTSDBClient client = new ArcadeTSDBClient();
        client.connect();
        Sender sender = new Sender() {
            int msgCnt = 0, metricCnt = 0;
            long preTime = -1;
            long logInterval = 10_000;
            Logger logger = Logger.getPureLogger("Expressway");

            public void count(int msg, int metric){
                msgCnt += msg;
                metricCnt += metric;
                if (preTime == -1)
                    preTime = System.currentTimeMillis();
                else{
                    long cur = System.currentTimeMillis();
                    if (cur - preTime > logInterval){
                        logger.logOnStdout("commit %d msg with %d metric in %d ms (%.2fIPS)", msgCnt, metricCnt, cur-preTime, metricCnt*1000.0/(cur-preTime));
                        msgCnt=0;
                        metricCnt=0;
                        preTime = cur;
                    }
                }
            }

            public void clearCount(){
                if (preTime != -1){
                    long cur = System.currentTimeMillis();
                    logger.logOnStdout("commit %d msg with %d metric in %d ms (%.2fIPS)", msgCnt, metricCnt, cur-preTime, metricCnt*1000.0/(cur-preTime));
                }
                msgCnt=0;
                metricCnt=0;
                preTime = -1;
            }

            @Override
            public void sendMsg(String mes) {
                JSONObject jsonMetrics = JSONObject.parseObject(mes);
                String gantryId = (String) jsonMetrics.remove("GANTRYID");
                JSONObject jsonDatapoint = new JSONObject();
                jsonDatapoint.put(ServerUtils.Key.TIMESERIES_TIMESTAMP, System.currentTimeMillis());
                jsonDatapoint.put(ServerUtils.Key.TIMESERIES_VALUE, jsonMetrics);
                JSONObject jsonInserts = new JSONObject();
                jsonInserts.put(ServerUtils.Key.INSERT_QUERY_OBJECT, OBJ_TYPE+":"+gantryId);
                jsonInserts.put(ServerUtils.Key.INSERT_TIMESERIES_FORMAT, ServerUtils.Value.TIMESERIES_FORMAT_DATAPOINT);
                jsonInserts.put(ServerUtils.Key.INSERT_TIMESERIES, jsonDatapoint);
                JSONObject jsonMsg = new JSONObject();
                jsonMsg.put(ServerUtils.Key.IN_ACTION, ServerUtils.Value.ACTION_TYPE_INSERT);
                jsonMsg.put(ServerUtils.Key.IN_DATABASE, DB_NAME);
                jsonMsg.put(ServerUtils.Key.IN_INSERT, jsonInserts);
                client.sendJsonAndWaitResult(jsonMsg);
                count(1, jsonMetrics.size());
            }

            @Override
            public void sendMsg(String topic, String mes) {
                System.out.println("not implemented");
            }
        };
        AdditionalSender.setSender(sender);
//        Config.VISUAL = false;
//        Config.SENDER_TYPE = "additional";
        Starter.main(new String[]{});
    }
}
