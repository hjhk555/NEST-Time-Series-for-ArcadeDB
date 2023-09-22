package nju.hjh.arcadedb.timeseries.server;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import nju.hjh.arcadedb.timeseries.*;
import nju.hjh.arcadedb.timeseries.datapoint.*;
import nju.hjh.arcadedb.timeseries.exception.*;
import nju.hjh.arcadedb.timeseries.statistics.*;
import nju.hjh.utils.exception.ExceptionSerializer;
import nju.hjh.utils.log.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.lang.reflect.Constructor;
import java.math.BigDecimal;
import java.net.InetAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ArcadeTSDBWorker implements Runnable{
    public static final ArrayList<String> METRIC_QUERY_TYPE = new ArrayList<>();
    public static final ArrayList<String> SINGLE_QUERY_TYPE = new ArrayList<>();
    public static final ArrayList<String> MULTIPLE_QUERY_TYPE = new ArrayList<>();

    static {
        METRIC_QUERY_TYPE.add(ServerUtils.Value.METRIC_QUERY_TYPE_LISTALL);
        METRIC_QUERY_TYPE.add(ServerUtils.Value.METRIC_QUERY_TYPE_FIRST);
        METRIC_QUERY_TYPE.add(ServerUtils.Value.METRIC_QUERY_TYPE_LAST);
        METRIC_QUERY_TYPE.add(ServerUtils.Value.METRIC_QUERY_TYPE_COUNT);
        METRIC_QUERY_TYPE.add(ServerUtils.Value.METRIC_QUERY_TYPE_MAX);
        METRIC_QUERY_TYPE.add(ServerUtils.Value.METRIC_QUERY_TYPE_MIN);
        METRIC_QUERY_TYPE.add(ServerUtils.Value.METRIC_QUERY_TYPE_SUM);
        METRIC_QUERY_TYPE.add(ServerUtils.Value.METRIC_QUERY_TYPE_AVERAGE);

        SINGLE_QUERY_TYPE.add(ServerUtils.Value.QUERY_TYPE_METRICS);
        SINGLE_QUERY_TYPE.add(ServerUtils.Value.METRIC_QUERY_TYPE_LISTALL);
        SINGLE_QUERY_TYPE.add(ServerUtils.Value.METRIC_QUERY_TYPE_FIRST);
        SINGLE_QUERY_TYPE.add(ServerUtils.Value.METRIC_QUERY_TYPE_LAST);
        SINGLE_QUERY_TYPE.add(ServerUtils.Value.METRIC_QUERY_TYPE_COUNT);
        SINGLE_QUERY_TYPE.add(ServerUtils.Value.METRIC_QUERY_TYPE_MAX);
        SINGLE_QUERY_TYPE.add(ServerUtils.Value.METRIC_QUERY_TYPE_MIN);
        SINGLE_QUERY_TYPE.add(ServerUtils.Value.METRIC_QUERY_TYPE_SUM);
        SINGLE_QUERY_TYPE.add(ServerUtils.Value.METRIC_QUERY_TYPE_AVERAGE);

        MULTIPLE_QUERY_TYPE.add(ServerUtils.Value.QUERY_TYPE_OBJECTS);
        MULTIPLE_QUERY_TYPE.add(ServerUtils.Value.METRIC_QUERY_TYPE_COUNT);
        MULTIPLE_QUERY_TYPE.add(ServerUtils.Value.METRIC_QUERY_TYPE_MAX);
        MULTIPLE_QUERY_TYPE.add(ServerUtils.Value.METRIC_QUERY_TYPE_MIN);
        MULTIPLE_QUERY_TYPE.add(ServerUtils.Value.METRIC_QUERY_TYPE_SUM);
        MULTIPLE_QUERY_TYPE.add(ServerUtils.Value.METRIC_QUERY_TYPE_AVERAGE);
    }

    public Socket socket;
    public final InetAddress remoteAddress;
    public final int remotePort;
    public final Logger logger;

    public ArcadeTSDBWorker(Socket socket, Logger serverLogger) {
        this.socket = socket;
        remoteAddress = socket.getInetAddress();
        remotePort = socket.getPort();
        logger = serverLogger.clone();
        logger.appendTag("remoteAddr", () -> String.format("remote: %s:%d", remoteAddress, remotePort));
    }

    @Override
    public void run() {
        logger.logOnStdout("worker handling socket connection");
        PrintWriter writer;
        BufferedReader reader;
        try {
            writer = new PrintWriter(socket.getOutputStream());
            reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        } catch (IOException e) {
            logger.logOnStderr("failed to create io from socket");
            throw new RuntimeException(e);
        }

        String msg;
        try {
            while (true) {
                msg = reader.readLine();
                if (msg == null){
                    Thread.sleep(1);
                }else {
                    logger.logOnStdout("msg received: %s", (msg.length() > ServerUtils.SHOW_MESSAGE_LENGTH ? msg.substring(0, ServerUtils.SHOW_MESSAGE_LENGTH) + " ...(total " + msg.length() + " characters)" : msg));
                    if (msg.equals(ServerUtils.CONNECTION_CLOSE))
                        break;

                    writer.write(handleMessage(msg).toJSONString()+"\n");
                    writer.flush();
                }
            }
        } catch (IOException e) {
            logger.logOnStderr("failed to read msg from client");
        } catch (InterruptedException e) {
            logger.logOnStderr("thread failed when waiting msg");
        }
        writer.write("close\n");
        writer.flush();

        try {
            reader.close();
            writer.close();
            socket.close();
        } catch (IOException ignored) {
        }

        logger.logOnStdout("connection closed");
    }

    private JSONObject handleMessage(String msg){
        JSONObject jsonRes = new JSONObject();
        JSONArray jsonExceptions = new JSONArray();

        try {
            JSONObject jsonMsg = JSONObject.parseObject(msg);
            String dbName = jsonMsg.getString(ServerUtils.Key.IN_DATABASE);
            if (dbName == null)
                dbName = ServerUtils.Value.DEFAULT_DATABASE;

            String action = jsonMsg.getString(ServerUtils.Key.IN_ACTION);
            if (action == null)
                throw new MissingFieldException(ServerUtils.Key.IN_ACTION);
            switch (action){
                case ServerUtils.Value.ACTION_TYPE_MANAGE -> {
                    String manageType = jsonMsg.getString(ServerUtils.Key.MANAGE_TYPE);
                    if (manageType == null)
                        throw new MissingFieldException(ServerUtils.Key.MANAGE_TYPE);

                    handleManage(dbName, manageType, jsonRes);
                }
                case ServerUtils.Value.ACTION_TYPE_INSERT -> {
                    // get rules
                    JSONObject strategies = jsonMsg.getJSONObject(ServerUtils.Key.IN_STRATEGY);
                    Map<String, UpdateStrategy> strategyMap = getStrategyMap(strategies);

                    JSONArray inserts = jsonMsg.getJSONArray(ServerUtils.Key.IN_INSERT);
                    if (inserts == null)
                        throw new MissingFieldException(ServerUtils.Key.IN_INSERT);

                    Database database = ArcadedbUtils.getOrCreateDatabase(dbName);
                    handleInsert(database, inserts, strategyMap, jsonRes, jsonExceptions);
                }
                case ServerUtils.Value.ACTION_TYPE_QUERY -> {
                    JSONArray queries = jsonMsg.getJSONArray(ServerUtils.Key.IN_QUERY);
                    if (queries == null)
                        throw new MissingFieldException(ServerUtils.Key.IN_QUERY);

                    Database database = ArcadedbUtils.getOrCreateDatabase(dbName);
                    handleQuery(database, queries, jsonRes, jsonExceptions);
                }
                default -> throw new MessageParsingException("invalid action type '"+action+"'");
            }
        }catch (Exception e){
            logger.logOnStderr(ExceptionSerializer.serializeAll(e));
            JSONObject jsonException = new JSONObject();
            jsonException.put(ServerUtils.Key.ERROR_CLASS, e.getClass().getName());
            jsonException.put(ServerUtils.Key.ERROR_MESSAGE, e.getMessage());
            jsonExceptions.add(jsonException);
        }

        if (!jsonExceptions.isEmpty())
            jsonRes.put(ServerUtils.Key.OUT_ERROR, jsonExceptions);

        return jsonRes;
    }

    private Map<String, UpdateStrategy> getStrategyMap(JSONObject strategies) throws MessageParsingException {
        HashMap<String, UpdateStrategy> strategyMap = new HashMap<>();
        if (strategies == null) return strategyMap;

        for (Map.Entry<String, Object> entry : strategies.entrySet()) {
            String metric = entry.getKey();
            if (entry.getValue() instanceof JSONObject jsonRule) {
                String strategyType = jsonRule.getString(ServerUtils.Key.STRATEGY_TYPE);
                if (strategyType == null)
                    strategyType = ServerUtils.Value.DEFAULT_STRATEGY_TYPE;

                String separator = jsonRule.getString(ServerUtils.Key.STRATEGY_SEPARATOR);
                if (separator == null)
                    separator = ServerUtils.Value.DEFAULT_SEPARATOR;

                UpdateStrategy strategy = switch (strategyType) {
                    case ServerUtils.Value.STRATEGY_TYPE_ERROR -> UpdateStrategy.ERROR;
                    case ServerUtils.Value.STRATEGY_TYPE_IGNORE -> UpdateStrategy.IGNORE;
                    case ServerUtils.Value.STRATEGY_TYPE_UPDATE -> UpdateStrategy.UPDATE;
                    case ServerUtils.Value.STRATEGY_TYPE_APPEND -> new UpdateStrategy(UpdateStrategy.TSBaseUpdateStrategy.APPEND, separator);
                    default -> throw new MessageParsingException("cannot resolve strategy '" + strategyType + "'");
                };

                strategyMap.put(metric, strategy);
            }
        }

        return strategyMap;
    }

    private String getTagDetail(Map<String, String> tags){
        if (tags == null || tags.isEmpty()) return "";
        StringBuilder tagDetail = new StringBuilder();
        boolean isFirstTag = true;
        for (Map.Entry<String, String> tag : tags.entrySet()){
            tagDetail.append(isFirstTag ? "(" : ",").append(String.format("%s=%s", tag.getKey(), tag.getValue()));
            isFirstTag = false;
        }
        return tagDetail.append(")").toString();
    }

    private void handleManage(String dbName, String manageType, JSONObject jsonResult){
        try {
            switch (manageType) {
                case ServerUtils.Value.MANAGE_TYPE_CREATE -> {
                    ArcadedbUtils.createDatabase(dbName);
                }
                case ServerUtils.Value.MANAGE_TYPE_DROP -> {
                    ArcadedbUtils.dropDatabase(dbName);
                }
                case ServerUtils.Value.MANAGE_TYPE_EXIST -> {
                    DatabaseFactory dbf = new DatabaseFactory(ServerUtils.DATABASE_DIR + dbName);
                    jsonResult.put(ServerUtils.Key.OUT_RESULT, dbf.exists());
                }
                default -> throw new MessageParsingException("unknown manage type '" + manageType + "'");
            }
            jsonResult.put(ServerUtils.Key.OUT_SUCCESS, true);
        } catch (Exception e) {
            jsonResult.put(ServerUtils.Key.OUT_RESULT, false);
            JSONObject jsonException = new JSONObject();
            jsonException.put(ServerUtils.Key.ERROR_CLASS, e.getClass().getName());
            jsonException.put(ServerUtils.Key.ERROR_MESSAGE, e.getMessage());
            jsonResult.put(ServerUtils.Key.OUT_ERROR, jsonException);
        }
    }

    private void handleInsert(Database database, JSONArray insertList, Map<String, UpdateStrategy> strategyMap, JSONObject jsonResult, JSONArray exceptionList){
        for (int index = 0; index < insertList.size(); index++){
            String retKey = null;
            try {
                JSONObject insert = insertList.getJSONObject(index);
                if (insert == null)
                    throw new MessageParsingException("cannot resolve insert #"+index);

                // object type
                String objectType = insert.getString(ServerUtils.Key.INSERT_QUERY_OBJECT);
                if (objectType == null)
                    throw new MessageParsingException("object type not found at insert #"+index);

                // tags
                JSONObject tagObject = insert.getJSONObject(ServerUtils.Key.INSERT_QUERY_TAG);
                HashMap<String, String> tags = new HashMap<>();
                if (tagObject != null){
                    for (Map.Entry<String, Object> tag : tagObject.entrySet()){
                        String tagKey = tag.getKey();
                        if (tag.getValue() instanceof String tagValue){
                            tags.put(tagKey, tagValue);
                        }else{
                            throw new MessageParsingException("tag '"+tagKey+"' has non-string value at insert #"+index);
                        }
                    }
                }

                // return key
                retKey = insert.getString(ServerUtils.Key.INSERT_QUERY_AS);
                if (retKey == null){
                    retKey = objectType + getTagDetail(tags);
                }
                if (jsonResult.containsKey(retKey)) {
                    retKey += "(#" + index + ")";
                }

                TimeseriesEngine tsEngine = TimeseriesEngine.getInstance(database);

                // start transaction
                synchronized (database) {
                    tsEngine.begin();

                    try {
                        // get object
                        MutableVertex object = ArcadedbUtils.getOrCreateSingleVertex(database, objectType, tags).modify();

                        JSONArray timeseries = insert.getJSONArray(ServerUtils.Key.INSERT_TIMESERIES);
                        if (timeseries != null) {
                            for (int tsIndex = 0; tsIndex < timeseries.size(); tsIndex++) {
                                JSONObject datapoint = timeseries.getJSONObject(tsIndex);
                                if (datapoint == null)
                                    throw new MessageParsingException("cannot resolve timeseries #" + tsIndex);

                                Long timestamp = datapoint.getLong(ServerUtils.Key.TIMESERIES_TIMESTAMP);
                                if (timestamp == null)
                                    throw new MessageParsingException("missing timestamp at timeseries #" + tsIndex);

                                JSONObject metrics = datapoint.getJSONObject(ServerUtils.Key.TIMESERIES_VALUE);
                                if (metrics != null) {
                                    for (Map.Entry<String, Object> metric : metrics.entrySet()) {
                                        String key = metric.getKey();
                                        Object value = metric.getValue();
                                        DataPoint dataPoint;
                                        DataType dataType;
                                        if (value instanceof Number numVal){
                                            if (value instanceof BigDecimal){
                                                dataType = DataType.DOUBLE;
                                                dataPoint = new DoubleDataPoint(timestamp, numVal.doubleValue());
                                            }else{
                                                dataType = DataType.LONG;
                                                dataPoint = new LongDataPoint(timestamp, numVal.longValue());
                                            }
                                        } else if (value instanceof String strVal) {
                                            dataType = DataType.STRING;
                                            dataPoint = new StringDataPoint(timestamp, strVal);
                                        } else {
                                            throw new MessageParsingException("unsupported data type of " + key + " at timeseries #" + tsIndex);
                                        }

                                        try {
                                            UpdateStrategy strategy = strategyMap.get(key);
                                            if (strategy == null)
                                                strategy = UpdateStrategy.IGNORE;
                                            tsEngine.insertDataPoint(object, key, dataType, dataPoint, strategy);
                                        } catch (TimeseriesException e){
                                            Constructor<? extends Exception> constructor;
                                            try{
                                                constructor = e.getClass().getConstructor(String.class);
                                            }catch (Exception ignored){
                                                throw e;
                                            }
                                            throw constructor.newInstance(String.format("%s%s:%s@%d: %s", objectType, getTagDetail(tags), key, timestamp, e.getMessage()));
                                        }
                                    }
                                }
                            }
                        }

                        tsEngine.commit();
                    }catch (Exception e){
                        tsEngine.rollback();
                        throw e;
                    }
                }

                JSONObject jsonSubResult = new JSONObject();
                jsonSubResult.put(ServerUtils.Key.OUT_SUCCESS, true);
                jsonResult.put(retKey, jsonSubResult);
            }catch (Exception e){
                logger.logOnStderr(ExceptionSerializer.serializeAll(e));
                JSONObject jsonException = new JSONObject();
                jsonException.put(ServerUtils.Key.ERROR_CLASS, e.getClass().getName());
                jsonException.put(ServerUtils.Key.ERROR_MESSAGE, e.getMessage());

                if (retKey != null){
                    JSONObject jsonSubResult = new JSONObject();
                    jsonSubResult.put(ServerUtils.Key.OUT_SUCCESS, false);
                    jsonSubResult.put(ServerUtils.Key.OUT_ERROR, jsonException);
                    jsonResult.put(retKey, jsonSubResult);
                }else {
                    exceptionList.add(jsonException);
                }
            }
        }
    }

    public void handleQuery(Database database, JSONArray queryList, JSONObject jsonResult, JSONArray exceptionList){
        for (int index = 0; index < queryList.size(); index++){
            String retKey = null;
            try{
                JSONObject query = queryList.getJSONObject(index);
                if (query == null)
                    throw new MessageParsingException("cannot resolve query #"+index);

                // object type
                String objectType = query.getString(ServerUtils.Key.INSERT_QUERY_OBJECT);
                if (objectType == null)
                    throw new MessageParsingException("object type not found at query #"+index);

                // query type
                String queryType = query.getString(ServerUtils.Key.QUERY_TYPE);
                if (queryType == null)
                    throw new MessageParsingException("query type not provided at query #"+index);
                boolean isSingleQuery = SINGLE_QUERY_TYPE.contains(queryType);
                boolean isMultipleQuery = MULTIPLE_QUERY_TYPE.contains(queryType);

                // multiple query
                Boolean isMultiple = query.getBoolean(ServerUtils.Key.QUERY_MULTIPLE);
                if (isMultiple == null)
                    isMultiple = ServerUtils.Value.DEFAULT_QUERY_MULTIPLE;
                // auto setting
                if (!isSingleQuery && !isMultipleQuery) throw new MessageParsingException("unknown query type "+queryType);
                if (!isMultipleQuery) isMultiple = false;
                if (!isSingleQuery) isMultiple = true;

                // metric
                // if this query requires metric name
                boolean isMetricQuery = METRIC_QUERY_TYPE.contains(queryType);
                String metric = query.getString(ServerUtils.Key.QUERY_METRIC);
                if (isMetricQuery && metric == null)
                    throw new MessageParsingException("metric not found for query type '"+queryType+"' at query #"+index);

                // tags
                JSONObject tagObject = query.getJSONObject(ServerUtils.Key.INSERT_QUERY_TAG);
                HashMap<String, String> tags = new HashMap<>();
                if (tagObject != null){
                    for (Map.Entry<String, Object> tag : tagObject.entrySet()){
                        String tagKey = tag.getKey();
                        if (tag.getValue() instanceof String tagValue){
                            tags.put(tagKey, tagValue);
                        }else{
                            throw new MessageParsingException("tag '"+tagKey+"' has non-string value at insert #"+index);
                        }
                    }
                }

                // return key
                retKey = query.getString(ServerUtils.Key.INSERT_QUERY_AS);
                if (retKey == null) {
                    retKey = objectType;
                    if (isMultiple) retKey += "(s)";
                    retKey += getTagDetail(tags);
                    if (isMetricQuery) retKey += "." + metric;
                    retKey += "." + queryType;
                }
                if (jsonResult.containsKey(retKey)) {
                    retKey += "(#" + index + ")";
                }

                JSONObject jsonSubResult;
                if (isMultiple)
                    jsonSubResult = getMultipleQueryResult(database, objectType, tags, queryType, metric, query);
                else
                    jsonSubResult = getSingleQueryResult(database, objectType, tags, queryType, metric, query);

                jsonResult.put(retKey, jsonSubResult);
            }catch (Exception e){
                logger.logOnStderr(ExceptionSerializer.serializeAll(e));
                JSONObject jsonException = new JSONObject();
                jsonException.put(ServerUtils.Key.ERROR_CLASS, e.getClass().getName());
                jsonException.put(ServerUtils.Key.ERROR_MESSAGE, e.getMessage());

                if (retKey != null){
                    JSONObject jsonSubResult = new JSONObject();
                    jsonSubResult.put(ServerUtils.Key.OUT_SUCCESS, false);
                    jsonSubResult.put(ServerUtils.Key.OUT_ERROR, jsonException);
                    jsonResult.put(retKey, jsonSubResult);
                }else {
                    exceptionList.add(jsonException);
                }
            }
        }
    }

    private JSONObject getSingleQueryResult(Database database, String objectType, Map<String, String> tags, String queryType, String metric, JSONObject jsonQuery) throws TimeseriesException {
        TimeseriesEngine tsEngine = TimeseriesEngine.getInstance(database);
        JSONObject jsonSubResult = new JSONObject();
        jsonSubResult.put(ServerUtils.Key.OUT_SUCCESS, true);
        // start transaction
        synchronized (database){
            tsEngine.begin();
            try{
                Vertex object = ArcadedbUtils.getSingleVertex(database, objectType, tags);
                if (queryType.equals(ServerUtils.Value.QUERY_TYPE_METRICS)){
                    JSONObject metrics = new JSONObject();
                    for (String metricName : tsEngine.getAllMetrics(object)){
                        Statistics stats = tsEngine.aggregativeQuery(object, metricName, 0, Long.MAX_VALUE);
                        if (stats.count == 0) continue;
                        if (stats instanceof FixedStatistics fixedStatistics){
                            metrics.put(metricName, fixedStatistics.getLastValue());
                        }else{
                            DataPointSet lastDPSet = tsEngine.periodQuery(object, metricName, stats.lastTime, stats.lastTime);
                            if (lastDPSet.hasNext()){
                                metrics.put(metricName, lastDPSet.next().getValue());
                            }else{
                                throw new TimeseriesException("last timestamp exist but data point not found at metric '"+metricName+"'");
                            }
                        }
                    }
                    jsonSubResult.put(ServerUtils.Key.OUT_RESULT, metrics);
                }else{
                    // collect timeseries query params
                    // begin time
                    Long beginTime = jsonQuery.getLong(ServerUtils.Key.QUERY_BEGIN_TIMESTAMP);
                    if (beginTime == null)
                        beginTime = 0L;
                    if (beginTime < 0)
                        throw new MessageParsingException("begin time should be 0 or positive");

                    // end time
                    Long endTime = jsonQuery.getLong(ServerUtils.Key.QUERY_END_TIMESTAMP);
                    if (endTime == null)
                        endTime = Long.MAX_VALUE;
                    if (endTime < beginTime)
                        throw new MessageParsingException("begin time "+beginTime+" larger than end time "+endTime);

                    switch (queryType){
                        case ServerUtils.Value.METRIC_QUERY_TYPE_LISTALL ->{
                            Long limit = jsonQuery.getLong(ServerUtils.Key.QUERY_LIMIT);
                            if (limit == null || limit > 1000)
                                limit = 1000L;
                            if (limit <= 0)
                                throw new MessageParsingException("limit should be positive");

                            JSONArray dataPointList = new JSONArray();
                            DataPointSet dataSet = tsEngine.periodQuery(object, metric, beginTime, endTime);
                            int count = 0;
                            while (count < limit && dataSet.hasNext()){
                                DataPoint nextDP = dataSet.next();
                                JSONObject jsonDataPoint = new JSONObject();
                                jsonDataPoint.put(ServerUtils.Key.OUT_TIMESTAMP, nextDP.timestamp);
                                jsonDataPoint.put(ServerUtils.Key.OUT_VALUE, nextDP.getValue());
                                dataPointList.add(jsonDataPoint);
                                count++;
                            }
                            jsonSubResult.put(ServerUtils.Key.OUT_RESULT, dataPointList);
                        }
                        case ServerUtils.Value.METRIC_QUERY_TYPE_COUNT ->{
                            Statistics stats = tsEngine.aggregativeQuery(object, metric, beginTime, endTime);
                            jsonSubResult.put(ServerUtils.Key.OUT_RESULT, stats.count);
                        }
                        case ServerUtils.Value.METRIC_QUERY_TYPE_FIRST ->{
                            Statistics stats = tsEngine.aggregativeQuery(object, metric, beginTime, endTime);
                            if (stats.count == 0) {
                                jsonSubResult.put(ServerUtils.Key.OUT_RESULT, null);
                                break;
                            }

                            JSONObject jsonDataPoint = new JSONObject();
                            long firstTime = stats.firstTime;
                            jsonDataPoint.put(ServerUtils.Key.OUT_TIMESTAMP, firstTime);
                            if (stats instanceof FixedStatistics fixedStats)
                                jsonDataPoint.put(ServerUtils.Key.OUT_VALUE, fixedStats.getFirstValue());
                            else{
                                DataPointSet firstDPSet = tsEngine.periodQuery(object, metric, firstTime, firstTime);
                                if (firstDPSet.hasNext()){
                                    jsonDataPoint.put(ServerUtils.Key.OUT_VALUE, firstDPSet.next().getValue());
                                }else{
                                    throw new TimeseriesException("first timestamp exist but data point not found");
                                }
                            }
                            jsonSubResult.put(ServerUtils.Key.OUT_RESULT, jsonDataPoint);
                        }
                        case ServerUtils.Value.METRIC_QUERY_TYPE_LAST ->{
                            Statistics stats = tsEngine.aggregativeQuery(object, metric, beginTime, endTime);
                            if (stats.count == 0) {
                                jsonSubResult.put(ServerUtils.Key.OUT_RESULT, null);
                                break;
                            }

                            JSONObject jsonDataPoint = new JSONObject();
                            long lastTime = stats.lastTime;
                            jsonDataPoint.put(ServerUtils.Key.OUT_TIMESTAMP, lastTime);
                            if (stats instanceof FixedStatistics fixedStats)
                                jsonDataPoint.put(ServerUtils.Key.OUT_VALUE, fixedStats.getLastValue());
                            else{
                                DataPointSet lastDPSet = tsEngine.periodQuery(object, metric, lastTime, lastTime);
                                if (lastDPSet.hasNext()){
                                    jsonDataPoint.put(ServerUtils.Key.OUT_VALUE, lastDPSet.next().getValue());
                                }else{
                                    throw new TimeseriesException("last timestamp exist but data point not found");
                                }
                            }
                            jsonSubResult.put(ServerUtils.Key.OUT_RESULT, jsonDataPoint);
                        }
                        case ServerUtils.Value.METRIC_QUERY_TYPE_SUM ->{
                            Statistics stats = tsEngine.aggregativeQuery(object, metric, beginTime, endTime);
                            if (stats instanceof NumericStatistics numStats){
                                jsonSubResult.put(ServerUtils.Key.OUT_RESULT, numStats.getSum());
                            }else{
                                throw new DataTypeMismatchException("metric '"+metric+"' is not numeric");
                            }
                        }
                        case ServerUtils.Value.METRIC_QUERY_TYPE_AVERAGE ->{
                            Statistics stats = tsEngine.aggregativeQuery(object, metric, beginTime, endTime);
                            if (stats instanceof NumericStatistics numStats){
                                jsonSubResult.put(ServerUtils.Key.OUT_RESULT, numStats.getSum().doubleValue()/numStats.count);
                            }else{
                                throw new DataTypeMismatchException("metric '"+metric+"' is not numeric");
                            }
                        }
                        case ServerUtils.Value.METRIC_QUERY_TYPE_MAX ->{
                            Statistics stats = tsEngine.aggregativeQuery(object, metric, beginTime, endTime);
                            if (stats instanceof NumericStatistics numStats){
                                jsonSubResult.put(ServerUtils.Key.OUT_RESULT, numStats.getMaxValue());
                            }else{
                                throw new DataTypeMismatchException("metric '"+metric+"' is not numeric");
                            }
                        }
                        case ServerUtils.Value.METRIC_QUERY_TYPE_MIN ->{
                            Statistics stats = tsEngine.aggregativeQuery(object, metric, beginTime, endTime);
                            if (stats instanceof NumericStatistics numStats){
                                jsonSubResult.put(ServerUtils.Key.OUT_RESULT, numStats.getMinValue());
                            }else{
                                throw new DataTypeMismatchException("metric '"+metric+"' is not numeric");
                            }
                        }
                        default -> throw new MessageParsingException("unsupported single query type '"+queryType+"'");
                    }
                }

                tsEngine.commit();
            }catch (TimeseriesException e){
                tsEngine.rollback();
                throw e;
            }
        }

        jsonSubResult.put(ServerUtils.Key.OUT_SUCCESS, true);
        return jsonSubResult;
    }

    private JSONObject getMultipleQueryResult(Database database, String objectType, Map<String, String> tags, String queryType, String metric, JSONObject jsonQuery) throws TimeseriesException{
        TimeseriesEngine tsEngine = TimeseriesEngine.getInstance(database);
        JSONObject jsonSubResult = new JSONObject();
        jsonSubResult.put(ServerUtils.Key.OUT_SUCCESS, true);
        // start transaction
        synchronized (database){
            tsEngine.begin();
            try{
                List<Vertex> objects = ArcadedbUtils.getVertices(database, objectType, tags);
                if (objects.size() == 0)
                    jsonSubResult.put(ServerUtils.Key.OUT_RESULT, null);
                else if (queryType.equals(ServerUtils.Value.QUERY_TYPE_OBJECTS)){
                    JSONArray objectList = new JSONArray();
                    for (Vertex object : objects){
                        StringBuilder taggedObjectBuilder = new StringBuilder(objectType);
                        Map<String, Object> objectTags = object.propertiesAsMap();
                        if (objectTags.size() == 0) taggedObjectBuilder.append("()");
                        else{
                            boolean isFirstTag = true;
                            for (Map.Entry<String, Object> tag : objectTags.entrySet()){
                                taggedObjectBuilder.append(isFirstTag ? "(" : ",").append(String.format("%s=%s", tag.getKey(), tag.getValue().toString()));
                                isFirstTag = false;
                            }
                            taggedObjectBuilder.append(")");
                        }
                        objectList.add(taggedObjectBuilder.toString());
                    }
                    jsonSubResult.put(ServerUtils.Key.OUT_RESULT, objectList);
                }else{
                    // collect timeseries query params
                    // begin time
                    Long beginTime = jsonQuery.getLong(ServerUtils.Key.QUERY_BEGIN_TIMESTAMP);
                    if (beginTime == null)
                        beginTime = 0L;
                    if (beginTime < 0)
                        throw new MessageParsingException("begin time should be 0 or positive");

                    // end time
                    Long endTime = jsonQuery.getLong(ServerUtils.Key.QUERY_END_TIMESTAMP);
                    if (endTime == null)
                        endTime = Long.MAX_VALUE;
                    if (endTime < beginTime)
                        throw new MessageParsingException("begin time "+beginTime+" larger than end time "+endTime);

                    switch (queryType){
                        case ServerUtils.Value.METRIC_QUERY_TYPE_COUNT ->{
                            long totalCount = 0;
                            for (Vertex object : objects) {
                                totalCount += tsEngine.aggregativeQuery(object, metric, beginTime, endTime).count;
                            }
                            jsonSubResult.put(ServerUtils.Key.OUT_RESULT, totalCount);
                        }
                        case ServerUtils.Value.METRIC_QUERY_TYPE_SUM ->{
                            long longSum = 0;
                            double doubleSum = 0;
                            boolean containDouble = false;

                            for (Vertex object : objects) {
                                Statistics stats = tsEngine.aggregativeQuery(object, metric, beginTime, endTime);
                                if (stats instanceof NumericStatistics numStats) {
                                    Number sum = numStats.getSum();
                                    if (!containDouble && numStats instanceof DoubleStatistics){
                                        doubleSum = longSum;
                                        containDouble = true;
                                    }
                                    if (containDouble)
                                        doubleSum += sum.doubleValue();
                                    else
                                        longSum += sum.longValue();
                                } else {
                                    throw new DataTypeMismatchException("metric '" + metric + "' is not numeric");
                                }
                            }
                            if (containDouble)
                                jsonSubResult.put(ServerUtils.Key.OUT_RESULT, doubleSum);
                            else
                                jsonSubResult.put(ServerUtils.Key.OUT_RESULT, longSum);
                        }
                        case ServerUtils.Value.METRIC_QUERY_TYPE_AVERAGE ->{
                            long totalCount = 0;
                            double doubleSum = 0;

                            for (Vertex object : objects) {
                                Statistics stats = tsEngine.aggregativeQuery(object, metric, beginTime, endTime);
                                if (stats instanceof NumericStatistics numStats) {
                                    doubleSum += numStats.getSum().doubleValue();
                                    totalCount += numStats.count;
                                } else {
                                    throw new DataTypeMismatchException("metric '" + metric + "' is not numeric");
                                }
                            }
                            jsonSubResult.put(ServerUtils.Key.OUT_RESULT, doubleSum/totalCount);
                        }
                        case ServerUtils.Value.METRIC_QUERY_TYPE_MAX ->{
                            Number totalMax = Double.MIN_VALUE;

                            for (Vertex object : objects) {
                                Statistics stats = tsEngine.aggregativeQuery(object, metric, beginTime, endTime);
                                if (stats instanceof NumericStatistics numStats) {
                                    Number max = numStats.getMaxValue();
                                    if (max.doubleValue() > totalMax.doubleValue()) totalMax = max;
                                } else {
                                    throw new DataTypeMismatchException("metric '" + metric + "' is not numeric");
                                }
                            }
                            jsonSubResult.put(ServerUtils.Key.OUT_RESULT, totalMax);
                        }
                        case ServerUtils.Value.METRIC_QUERY_TYPE_MIN ->{
                            Number totalMin = Double.MAX_VALUE;

                            for (Vertex object : objects) {
                                Statistics stats = tsEngine.aggregativeQuery(object, metric, beginTime, endTime);
                                if (stats instanceof NumericStatistics numStats) {
                                    Number min = numStats.getMinValue();
                                    if (totalMin.doubleValue() > min.doubleValue()) totalMin = min;
                                } else {
                                    throw new DataTypeMismatchException("metric '" + metric + "' is not numeric");
                                }
                            }
                            jsonSubResult.put(ServerUtils.Key.OUT_RESULT, totalMin);
                        }
                        default -> throw new MessageParsingException("unsupported multiple query type '"+queryType+"'");
                    }
                }

                tsEngine.commit();
            }catch (TimeseriesException e){
                tsEngine.rollback();
                throw e;
            }
        }

        return jsonSubResult;
    }
}
