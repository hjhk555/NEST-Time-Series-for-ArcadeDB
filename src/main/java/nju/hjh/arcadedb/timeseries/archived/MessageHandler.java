package nju.hjh.arcadedb.timeseries.archived;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.RID;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.ResultSet;
import nju.hjh.arcadedb.timeseries.DataPointList;
import nju.hjh.arcadedb.timeseries.DataType;
import nju.hjh.arcadedb.timeseries.NestEngine;
import nju.hjh.arcadedb.timeseries.UpdateStrategy;
import nju.hjh.arcadedb.timeseries.datapoint.DataPoint;
import nju.hjh.arcadedb.timeseries.datapoint.DoubleDataPoint;
import nju.hjh.arcadedb.timeseries.datapoint.LongDataPoint;
import nju.hjh.arcadedb.timeseries.datapoint.StringDataPoint;
import nju.hjh.arcadedb.timeseries.exception.*;
import nju.hjh.arcadedb.timeseries.server.utils.DatabaseUtils;
import nju.hjh.arcadedb.timeseries.statistics.DoubleStatistics;
import nju.hjh.arcadedb.timeseries.statistics.FixedStatistics;
import nju.hjh.arcadedb.timeseries.statistics.NumericStatistics;
import nju.hjh.arcadedb.timeseries.statistics.Statistics;
import nju.hjh.utils.exception.ExceptionSerializer;
import nju.hjh.utils.log.Logger;

import java.math.BigDecimal;
import java.util.*;

public class MessageHandler {
    public static final ArrayList<String> NO_METRIC_QUERY = new ArrayList<>();
    public static final ArrayList<String> SEPARATE_ONLY_QUERY = new ArrayList<>();
    public static final ArrayList<String> MULTIPLE_ONLY_QUERY = new ArrayList<>();

    static {
        NO_METRIC_QUERY.add(ServerUtils.Value.QUERY_TYPE_INFO);

        SEPARATE_ONLY_QUERY.add(ServerUtils.Value.QUERY_TYPE_INFO);
        SEPARATE_ONLY_QUERY.add(ServerUtils.Value.METRIC_QUERY_TYPE_LISTALL);
    }

    private Logger logger;

    public MessageHandler(Logger logger){
        this.logger = logger;
    }

    public JSONObject handleMessage(String msg) {
        try {
            JSONObject jsonMsg = JSONObject.parseObject(msg);
            String dbName = jsonMsg.getString(ServerUtils.Key.IN_DATABASE);
            if (dbName == null)
                dbName = ServerUtils.Value.DEFAULT_DATABASE;

            String action = jsonMsg.getString(ServerUtils.Key.IN_ACTION);
            if (action == null)
                throw new MissingFieldException(ServerUtils.Key.IN_ACTION);
            switch (action) {
                case ServerUtils.Value.ACTION_TYPE_MANAGE -> {
                    String manageType = jsonMsg.getString(ServerUtils.Key.MANAGE_TYPE);
                    if (manageType == null)
                        throw new MissingFieldException(ServerUtils.Key.MANAGE_TYPE);

                    return handleManage(dbName, manageType);
                }
                case ServerUtils.Value.ACTION_TYPE_INSERT -> {
                    // get rules
                    JSONObject strategies = jsonMsg.getJSONObject(ServerUtils.Key.IN_STRATEGY);
                    Map<String, UpdateStrategy> strategyMap = getStrategyMap(strategies);

                    JSONArray inserts = jsonMsg.getJSONArray(ServerUtils.Key.IN_INSERT);
                    if (inserts == null)
                        throw new MissingFieldException(ServerUtils.Key.IN_INSERT);

                    Database database = DatabaseUtils.getOrCreateDatabase(dbName);
                    return handleInsert(database, inserts, strategyMap);
                }
                case ServerUtils.Value.ACTION_TYPE_QUERY -> {
                    JSONObject query = jsonMsg.getJSONObject(ServerUtils.Key.IN_QUERY);
                    if (query == null)
                        throw new MissingFieldException(ServerUtils.Key.IN_QUERY);

                    Database database = DatabaseUtils.getDatabase(dbName);
                    return handleQuery(database, query);
                }
                default -> throw new MessageParsingException("invalid action type '" + action + "'");
            }
        } catch (Exception e) {
            logger.logOnStderr(ExceptionSerializer.serializeAll(e));
            JSONObject jsonRes = new JSONObject();
            jsonRes.put(ServerUtils.Key.OUT_SUCCESS, false);
            JSONObject jsonException = new JSONObject();
            jsonException.put(ServerUtils.Key.ERROR_CLASS, e.getClass().getName());
            jsonException.put(ServerUtils.Key.ERROR_MESSAGE, e.getMessage());
            jsonRes.put(ServerUtils.Key.OUT_ERROR, jsonException);
            return jsonRes;
        }
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
                    case ServerUtils.Value.STRATEGY_TYPE_APPEND ->
                            new UpdateStrategy(UpdateStrategy.TSBaseUpdateStrategy.APPEND, separator);
                    default -> throw new MessageParsingException("cannot resolve strategy '" + strategyType + "'");
                };
                strategyMap.put(metric, strategy);
            }
        }
        return strategyMap;
    }

    private JSONObject handleManage(String dbName, String manageType) throws TimeseriesException {
        JSONObject jsonRes = new JSONObject();
        jsonRes.put(ServerUtils.Key.OUT_SUCCESS, true);

        switch (manageType) {
            case ServerUtils.Value.MANAGE_TYPE_CREATE -> {
                DatabaseUtils.createDatabase(dbName);
            }
            case ServerUtils.Value.MANAGE_TYPE_DROP -> {
                DatabaseUtils.dropDatabase(dbName);
            }
            case ServerUtils.Value.MANAGE_TYPE_EXIST -> {
                DatabaseFactory dbf = new DatabaseFactory(ServerUtils.DATABASE_DIR + dbName);
                jsonRes.put(ServerUtils.Key.OUT_RESULT, dbf.exists());
            }
            default -> throw new MessageParsingException("unknown manage type '" + manageType + "'");
        }
        return jsonRes;
    }

    private JSONObject handleInsert(Database database, JSONArray insertList, Map<String, UpdateStrategy> strategyMap) throws Exception {
        NestEngine tsEngine = NestEngine.getInstance(database);
        // start transaction
        synchronized (database) {
            tsEngine.begin();
            try {
                for (int insertIndex = 0; insertIndex < insertList.size(); insertIndex++) {
                    JSONObject insert = insertList.getJSONObject(insertIndex);
                    if (insert == null)
                        throw new MessageParsingException("cannot parse insert#" + insertIndex + " as json object");

                    // object type and id
                    String objectTypeId = insert.getString(ServerUtils.Key.INSERT_QUERY_OBJECT);
                    if (objectTypeId == null)
                        throw new MissingFieldException(ServerUtils.Key.INSERT_QUERY_OBJECT, "insert#" + insertIndex);
                    int sepIndex = objectTypeId.indexOf(':');
                    if (sepIndex == -1)
                        throw new MessageParsingException("object id "+objectTypeId+" syntax error: objectType:objectId");
                    String objectType = objectTypeId.substring(0, sepIndex);
                    if (objectType.length() == 0)
                        throw new MessageParsingException("object id "+objectTypeId+" syntax error: empty type");
                    String objectId = objectTypeId.substring(sepIndex+1);
                    if (objectId.length() == 0)
                        throw new MessageParsingException("object id "+objectTypeId+" syntax error: empty id");

                    // tags
                    JSONObject tagObject = insert.getJSONObject(ServerUtils.Key.INSERT_TAG);
                    HashMap<String, String> tags = new HashMap<>();
                    if (tagObject != null) {
                        for (Map.Entry<String, Object> tag : tagObject.entrySet()) {
                            String tagKey = tag.getKey();
                            if (tagKey.equals(DatabaseUtils.PROP_OBJECT_ID))
                                throw new MessageParsingException("cannot use '"+ DatabaseUtils.PROP_OBJECT_ID+"' as tag key");

                            if (tagKey.startsWith(NestEngine.PREFIX_METRIC))
                                throw new MessageParsingException("cannot use '"+ NestEngine.PREFIX_METRIC+"' as tag prefix");

                            if (tag.getValue() instanceof String tagValue) {
                                tags.put(tagKey, tagValue);
                            } else {
                                throw new MessageParsingException("tag '" + tagKey + "' has non-string value at insert#" + insertIndex);
                            }
                        }
                    }

                    // get object
                    MutableVertex object = DatabaseUtils.getOrCreateSingleVertex(database, objectType, objectId, tags).modify();
                    Boolean tagOverwrite = insert.getBoolean(ServerUtils.Key.INSERT_TAG_OVERWRITE);
                    if (tagOverwrite!=null && tagOverwrite){
                        for (Map.Entry<String, String> tag : tags.entrySet()){
                            object.set(tag.getKey(), tag.getValue());
                        }
                        object.save();
                    }

                    String format = insert.getString(ServerUtils.Key.INSERT_TIMESERIES_FORMAT);
                    if (format == null)
                        throw new MissingFieldException(ServerUtils.Key.INSERT_TIMESERIES_FORMAT, String.format("insert#%d", insertIndex));

                    switch (format) {
                        case ServerUtils.Value.TIMESERIES_FORMAT_DATAPOINT -> {
                            JSONArray datapoints = insert.getJSONArray(ServerUtils.Key.INSERT_TIMESERIES);
                            if (datapoints == null)
                                throw new MissingFieldException(ServerUtils.Key.INSERT_TIMESERIES, String.format("insert#%d", insertIndex));
                            insertDatapointFormatTimeseries(tsEngine, object, datapoints, strategyMap, insertIndex);
                        }
                        case ServerUtils.Value.TIMESERIES_FORMAT_TIMEVALUE_ARRAY -> {
                            JSONObject metrics = insert.getJSONObject(ServerUtils.Key.INSERT_TIMESERIES);
                            if (metrics == null)
                                throw new MissingFieldException(ServerUtils.Key.INSERT_TIMESERIES, String.format("insert#%d", insertIndex));
                            insertArrayFormatTimeseries(tsEngine, object, metrics, strategyMap, insertIndex);
                        }
                        case ServerUtils.Value.TIMESERIES_FORMAT_TIMEVALUE_TABLE -> {
                            JSONObject timeMetrics = insert.getJSONObject(ServerUtils.Key.INSERT_TIMESERIES);
                            if (timeMetrics == null)
                                throw new MissingFieldException(ServerUtils.Key.INSERT_TIMESERIES, String.format("insert#%d", insertIndex));
                            insertTableFormatTimeseries(tsEngine, object, timeMetrics, strategyMap, insertIndex);
                        }
                        case ServerUtils.Value.TIMESERIES_FORMAT_NULL -> {
                            break;
                        }
                        default ->
                                throw new MessageParsingException(String.format("unsupported timeseries format at insert#%d", insertIndex));
                    }
                }
                tsEngine.commit();
            } catch (Exception e) {
                tsEngine.rollback();
                throw e;
            }
        }

        JSONObject jsonRes = new JSONObject();
        jsonRes.put(ServerUtils.Key.OUT_SUCCESS, true);
        return jsonRes;
    }

    public void insertArrayFormatTimeseries(NestEngine tsEngine, MutableVertex object, JSONObject metrics, Map<String, UpdateStrategy> strategyMap, int insertIndex) throws Exception {
        for (String metricName : metrics.keySet()) {
            JSONObject timeValue = metrics.getJSONObject(metricName);
            if (timeValue == null)
                throw new MessageParsingException(String.format("cannot parse insert#%d.%s as json object", insertIndex, metricName));

            JSONArray timestamps = timeValue.getJSONArray(ServerUtils.Key.TIMESERIES_TIMESTAMP);
            if (timestamps == null)
                throw new MissingFieldException(ServerUtils.Key.TIMESERIES_TIMESTAMP, String.format("insert#%d.%s", insertIndex, metricName));

            JSONArray values = timeValue.getJSONArray(ServerUtils.Key.TIMESERIES_VALUE);
            if (values == null)
                throw new MissingFieldException(ServerUtils.Key.TIMESERIES_VALUE, String.format("insert#%d.%s", insertIndex, metricName));

            long metricSize = timestamps.size();

            if (metricSize != values.size())
                throw new MessageParsingException("timestamp and value have different size at insert#" + insertIndex + "." + metricName);

            for (int metricIndex = 0; metricIndex < metricSize; metricIndex++) {
                Long timestamp = timestamps.getLong(metricIndex);
                if (timestamp == null) continue;
                Object value = values.get(metricIndex);
                if (value == null) continue;

                DataPoint dataPoint;
                DataType dataType;
                if (value instanceof Number numVal) {
                    if (numVal instanceof BigDecimal) {
                        dataType = DataType.DOUBLE;
                        dataPoint = new DoubleDataPoint(timestamp, numVal.doubleValue());
                    } else {
                        dataType = DataType.LONG;
                        dataPoint = new LongDataPoint(timestamp, numVal.longValue());
                    }
                } else if (value instanceof String strVal) {
                    dataType = DataType.STRING;
                    dataPoint = new StringDataPoint(timestamp, strVal);
                } else {
                    throw new MessageParsingException(String.format("unsupported value(=%s) at insert#%d.%s[%d]", value, insertIndex, metricName, metricIndex));
                }

                try {
                    UpdateStrategy strategy = strategyMap.get(metricName);
                    if (strategy == null)
                        strategy = UpdateStrategy.IGNORE;
                    tsEngine.insertDataPoint(object, metricName, dataType, dataPoint, strategy);
                }catch (TimeseriesException e){
                    throw new TimeseriesException(String.format("%s:%s.%s[%d]: %s", object.getTypeName(), object.getString(DatabaseUtils.PROP_OBJECT_ID), metricName, timestamp, e.getMessage()));
                }
            }
        }
    }

    public void insertDatapointFormatTimeseries(NestEngine tsEngine, MutableVertex object, JSONArray datapoints, Map<String, UpdateStrategy> strategyMap, int insertIndex) throws Exception {
        for (int dpIndex = 0; dpIndex < datapoints.size(); dpIndex++) {
            JSONObject datapoint = datapoints.getJSONObject(dpIndex);
            if (datapoint == null)
                throw new MessageParsingException(String.format("cannot parse insert#%d[%d] as json object", insertIndex, dpIndex));

            Long timestamp = datapoint.getLong(ServerUtils.Key.TIMESERIES_TIMESTAMP);
            if (timestamp == null)
                throw new MissingFieldException(ServerUtils.Key.TIMESERIES_TIMESTAMP, String.format("insert#%d[%d]", insertIndex, dpIndex));

            JSONObject metrics = datapoint.getJSONObject(ServerUtils.Key.TIMESERIES_VALUE);
            if (metrics == null)
                throw new MissingFieldException(ServerUtils.Key.TIMESERIES_VALUE, String.format("insert#%d[%d]", insertIndex, dpIndex));

            for (Map.Entry<String, Object> metric : metrics.entrySet()) {
                String metricName = metric.getKey();
                Object value = metric.getValue();
                if (value == null) continue;

                DataPoint dataPoint;
                DataType dataType;
                if (value instanceof Number numVal) {
                    if (numVal instanceof BigDecimal) {
                        dataType = DataType.DOUBLE;
                        dataPoint = new DoubleDataPoint(timestamp, numVal.doubleValue());
                    } else {
                        dataType = DataType.LONG;
                        dataPoint = new LongDataPoint(timestamp, numVal.longValue());
                    }
                } else if (value instanceof String strVal) {
                    dataType = DataType.STRING;
                    dataPoint = new StringDataPoint(timestamp, strVal);
                } else {
                    throw new MessageParsingException(String.format("unsupported value(=%s) at insert#%d[%d].%s", value, insertIndex, dpIndex, metricName));
                }

                try {
                    UpdateStrategy strategy = strategyMap.get(metricName);
                    if (strategy == null)
                        strategy = UpdateStrategy.IGNORE;
                    tsEngine.insertDataPoint(object, metricName, dataType, dataPoint, strategy);
                } catch (TimeseriesException e) {
                    throw new TimeseriesException(String.format("%s:%s.%s[%d]: %s", object.getTypeName(), object.getString(DatabaseUtils.PROP_OBJECT_ID), metricName, timestamp, e.getMessage()));
                }
            }
        }
    }

    public void insertTableFormatTimeseries(NestEngine tsEngine, MutableVertex object, JSONObject timeMetrics, Map<String, UpdateStrategy> strategyMap, int insertIndex) throws Exception {
        JSONArray jsonTimestamps = timeMetrics.getJSONArray(ServerUtils.Key.TIMESERIES_TIMESTAMP);
        if (jsonTimestamps == null)
            throw new MissingFieldException(ServerUtils.Key.TIMESERIES_TIMESTAMP, String.format("insert#%d", insertIndex));

        int timeSize = jsonTimestamps.size();
        long[] timestamps = new long[timeSize];
        for (int tIndex=0; tIndex<timeSize; tIndex++) {
            Long timestamp = jsonTimestamps.getLong(tIndex);
            if (timestamp == null)
                throw new MessageParsingException("failed to parse timestamp "+jsonTimestamps.get(tIndex)+" at insert#"+insertIndex);
            timestamps[tIndex] = timestamp;
        }

        JSONObject metricSet = timeMetrics.getJSONObject(ServerUtils.Key.TIMESERIES_VALUE);
        if (metricSet == null)
            throw new MissingFieldException(ServerUtils.Key.TIMESERIES_VALUE, String.format("insert#%d", insertIndex));

        for (String metricName :metricSet.keySet()) {
            JSONArray metrics = metricSet.getJSONArray(metricName);
            if (metrics == null)
                throw new MessageParsingException(String.format("cannot parse insert#%d.%s as json array", insertIndex, metricName));
            if (metrics.size() != timeSize)
                throw new MessageParsingException(String.format("metric size different from timestamp at insert#%d.%s", insertIndex, metricName));

            for (int mIndex=0; mIndex<timeSize; mIndex++) {
                long timestamp = timestamps[mIndex];
                Object value = metrics.get(mIndex);
                if (value == null) continue;

                DataPoint dataPoint;
                DataType dataType;
                if (value instanceof Number numVal) {
                    if (numVal instanceof BigDecimal) {
                        dataType = DataType.DOUBLE;
                        dataPoint = new DoubleDataPoint(timestamp, numVal.doubleValue());
                    } else {
                        dataType = DataType.LONG;
                        dataPoint = new LongDataPoint(timestamp, numVal.longValue());
                    }
                } else if (value instanceof String strVal) {
                    dataType = DataType.STRING;
                    dataPoint = new StringDataPoint(timestamp, strVal);
                } else {
                    throw new MessageParsingException(String.format("unsupported value(=%s) at insert#%d.%s[%d]", value, insertIndex, metricName, mIndex));
                }

                try {
                    UpdateStrategy strategy = strategyMap.get(metricName);
                    if (strategy == null)
                        strategy = UpdateStrategy.IGNORE;
                    tsEngine.insertDataPoint(object, metricName, dataType, dataPoint, strategy);
                } catch (TimeseriesException e) {
                    throw new TimeseriesException(String.format("%s:%s.%s[%d]: %s", object.getTypeName(), object.getString(DatabaseUtils.PROP_OBJECT_ID), metricName, timestamp, e.getMessage()));
                }
            }
        }
    }

    private void parseObjects(Database database, String objectIdentifier, Set<Vertex> objects) throws TimeseriesException {
        objectIdentifier = objectIdentifier.trim();
        if (objectIdentifier.length() == 0) return;

        if (!objectIdentifier.endsWith("}"))
            throw new MessageParsingException("not end with }");

        if (objectIdentifier.substring(0, ServerUtils.Value.QUERY_OBJECT_PREFIX_RID.length()).equalsIgnoreCase(ServerUtils.Value.QUERY_OBJECT_PREFIX_RID)) {
            String rid = objectIdentifier.substring(ServerUtils.Value.QUERY_OBJECT_PREFIX_RID.length(), objectIdentifier.length()-1);
            int sepIndex = rid.indexOf(":");
            if (sepIndex == -1)
                throw new MessageParsingException("rid syntax error");

            String strBucketId = rid.substring(0, sepIndex);
            if (strBucketId.length() == 0)
                throw new MessageParsingException("empty bucketId");
            String strOffset = rid.substring(sepIndex + 1);
            if (strOffset.length() == 0)
                throw new MessageParsingException("empty offset");

            objects.add(database.lookupByRID(new RID(database, Integer.parseInt(strBucketId), Long.parseLong(strOffset)), true).asVertex());
        }else if (objectIdentifier.substring(0, ServerUtils.Value.QUERY_OBJECT_PREFIX_OBJECT_ID.length()).equalsIgnoreCase(ServerUtils.Value.QUERY_OBJECT_PREFIX_OBJECT_ID)){
            String objectTypeId = objectIdentifier.substring(ServerUtils.Value.QUERY_OBJECT_PREFIX_OBJECT_ID.length(), objectIdentifier.length()-1);
            int sepIndex = objectTypeId.indexOf(":");
            if (sepIndex == -1)
                throw new MessageParsingException(String.format("cannot get object from %s: object id syntax error", objectIdentifier));
            String objectType = objectTypeId.substring(0, sepIndex);
            if (objectType.length() == 0)
                throw new MessageParsingException("empty type");
            String objectId = objectTypeId.substring(sepIndex+1);
            if (objectId.length() == 0)
                throw new MessageParsingException("empty id");

            objects.add(DatabaseUtils.getSingleVertex(database, objectType, objectId));
        }else if (objectIdentifier.substring(0, ServerUtils.Value.QUERY_OBJECT_PREFIX_SQL.length()).equalsIgnoreCase(ServerUtils.Value.QUERY_OBJECT_PREFIX_SQL)){
            String sql = objectIdentifier.substring(ServerUtils.Value.QUERY_OBJECT_PREFIX_SQL.length(), objectIdentifier.length()-1);
            ResultSet results = database.query("SQL", sql);

            while (results.hasNext()){
                results.next().getVertex().ifPresent(objects::add);
            }
        }else
            throw new MessageParsingException("unsupported object type");
    }

    private JSONObject handleQuery(Database database, JSONObject query) throws TimeseriesException {
        // objects
        String objectIdentifiers = query.getString(ServerUtils.Key.INSERT_QUERY_OBJECT);
        if (objectIdentifiers == null)
            throw new MissingFieldException(ServerUtils.Key.INSERT_QUERY_OBJECT, "query detail");

        // query type
        String queryType = query.getString(ServerUtils.Key.QUERY_TYPE);
        if (queryType == null)
            throw new MissingFieldException(ServerUtils.Key.QUERY_TYPE, "query detail");

        // multiple query
        Boolean isMultiple = query.getBoolean(ServerUtils.Key.QUERY_MULTIPLE);
        if (isMultiple == null)
            isMultiple = ServerUtils.Value.DEFAULT_QUERY_MULTIPLE;
        // auto setting
        if (SEPARATE_ONLY_QUERY.contains(queryType)) isMultiple = false;
        else if (MULTIPLE_ONLY_QUERY.contains(queryType)) isMultiple = true;

        // metric
        // if this query requires metric name
        boolean isMetricQuery = !NO_METRIC_QUERY.contains(queryType);
        String metric = query.getString(ServerUtils.Key.QUERY_METRIC);
        if (isMetricQuery && metric == null)
            throw new MissingFieldException(ServerUtils.Key.QUERY_METRIC, "query detail");


        synchronized (database) {
            HashSet<Vertex> objects = new HashSet<>();
            for (String objectIdentifier : objectIdentifiers.split(",")) {
                try {
                    parseObjects(database, objectIdentifier, objects);
                }catch (Exception e){
                    throw new MessageParsingException(String.format("cannot get object(s) from %s: %s", objectIdentifier, e.getMessage()));
                }
            }

            if (isMultiple)
                return getMultipleQueryResult(database, objects, queryType, metric, query);
            else
                return getSeparateQueryResult(database, objects, queryType, metric, query);
        }
    }

    private JSONObject getSeparateQueryResult(Database database, Set<Vertex> objects, String queryType, String metric, JSONObject jsonQuery) throws TimeseriesException {
        NestEngine tsEngine = NestEngine.getInstance(database);
        JSONObject jsonResult = new JSONObject();

        if (queryType.equals(ServerUtils.Value.QUERY_TYPE_INFO)) {
            int nullCount = 0;
            for (Vertex object : objects) {
                String objectId = object.getString(DatabaseUtils.PROP_OBJECT_ID);
                if (objectId == null) objectId = String.format("null#%d", nullCount++);
                String objectKey = object.getTypeName()+":"+objectId;

                JSONObject objectInfo = new JSONObject();
                JSONObject tags = new JSONObject();
                for (Map.Entry<String, Object> prop : object.toMap(false).entrySet()){
                    if (!prop.getKey().equals(DatabaseUtils.PROP_OBJECT_ID) && !prop.getKey().startsWith(NestEngine.PREFIX_METRIC))
                        tags.put(prop.getKey(), prop.getValue());
                }
                objectInfo.put(ServerUtils.Key.OUT_TAG, tags);

                JSONObject metrics = new JSONObject();
                for (String metricName : tsEngine.getAllMetrics(object)) {
                    Statistics stats = tsEngine.aggregativeQuery(object, metricName, 0, Long.MAX_VALUE);
                    if (stats.count == 0) continue;
                    if (stats instanceof FixedStatistics fixedStatistics) {
                        metrics.put(metricName, fixedStatistics.getLastValue());
                    } else {
                        DataPointList lastDPSet = tsEngine.periodQuery(object, metricName, stats.lastTime, stats.lastTime, 1);
                        if (lastDPSet.hasNext()) {
                            metrics.put(metricName, lastDPSet.next().getValue());
                        } else {
                            throw new TimeseriesException("last timestamp exist but data point not found at metric '" + metricName + "'");
                        }
                    }
                }
                objectInfo.put(ServerUtils.Key.OUT_METRIC, metrics);
                jsonResult.put(objectKey, objectInfo);
            }
        } else {
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
                throw new MessageParsingException("begin time " + beginTime + " larger than end time " + endTime);

            if (queryType.equals(ServerUtils.Value.METRIC_QUERY_TYPE_LISTALL)) {
                Integer limit = jsonQuery.getInteger(ServerUtils.Key.QUERY_LIMIT);
                if (limit == null || limit > ServerUtils.MAX_LISTALL_SIZE)
                    limit = ServerUtils.MAX_LISTALL_SIZE;
                if (limit <= 0)
                    throw new MessageParsingException("limit should be positive");


                int nullCount = 0;
                for (Vertex object : objects) {
                    String objectId = object.getString(DatabaseUtils.PROP_OBJECT_ID);
                    if (objectId == null) objectId = String.format("null#%d", nullCount++);
                    String objectKey = String.format("%s:%s.%s[%d~%d]", object.getTypeName(), objectId, metric, beginTime, endTime);

                    try {
                        JSONArray timestamps = new JSONArray();
                        JSONArray values = new JSONArray();
                        for (DataPoint dp : tsEngine.periodQuery(object, metric, beginTime, endTime, limit).getList()) {
                            timestamps.add(dp.timestamp);
                            values.add(dp.getValue());
                        }
                        JSONObject dpList = new JSONObject();
                        dpList.put(ServerUtils.Key.TIMESERIES_TIMESTAMP, timestamps);
                        dpList.put(ServerUtils.Key.TIMESERIES_VALUE, values);
                        jsonResult.put(objectKey, dpList);
                    } catch (TargetNotFoundException e) {
                        jsonResult.put(objectKey, null);
                    }
                }
            } else {
                // aggressive
                int nullCount = 0;
                for (Vertex object : objects) {
                    String objectId = object.getString(DatabaseUtils.PROP_OBJECT_ID);
                    if (objectId == null) objectId = String.format("null#%d", nullCount++);
                    String objectKey = String.format("%s:%s.%s(%s[%d~%d])", object.getTypeName(), objectId, queryType, metric, beginTime, endTime);

                    try {
                        switch (queryType) {
                            case ServerUtils.Value.METRIC_QUERY_TYPE_COUNT -> {
                                Statistics stats = tsEngine.aggregativeQuery(object, metric, beginTime, endTime);
                                jsonResult.put(objectKey, stats.count);
                            }
                            case ServerUtils.Value.METRIC_QUERY_TYPE_FIRST -> {
                                Statistics stats = tsEngine.aggregativeQuery(object, metric, beginTime, endTime);
                                if (stats.count == 0) {
                                    jsonResult.put(objectKey, null);
                                    break;
                                }

                                JSONObject jsonDataPoint = new JSONObject();
                                long firstTime = stats.firstTime;
                                jsonDataPoint.put(ServerUtils.Key.OUT_TIMESTAMP, firstTime);
                                if (stats instanceof FixedStatistics fixedStats)
                                    jsonDataPoint.put(ServerUtils.Key.OUT_VALUE, fixedStats.getFirstValue());
                                else {
                                    DataPointList firstDPSet = tsEngine.periodQuery(object, metric, firstTime, firstTime, 1);
                                    if (firstDPSet.hasNext()) {
                                        jsonDataPoint.put(ServerUtils.Key.OUT_VALUE, firstDPSet.next().getValue());
                                    } else {
                                        throw new TimeseriesException(objectKey+" contains first timestamp but data point not found");
                                    }
                                }``
                                jsonResult.put(objectKey, jsonDataPoint);
                            }
                            case ServerUtils.Value.METRIC_QUERY_TYPE_LAST -> {
                                Statistics stats = tsEngine.aggregativeQuery(object, metric, beginTime, endTime);
                                if (stats.count == 0) {
                                    jsonResult.put(objectKey, null);
                                    break;
                                }

                                JSONObject jsonDataPoint = new JSONObject();
                                long lastTime = stats.lastTime;
                                jsonDataPoint.put(ServerUtils.Key.OUT_TIMESTAMP, lastTime);
                                if (stats instanceof FixedStatistics fixedStats)
                                    jsonDataPoint.put(ServerUtils.Key.OUT_VALUE, fixedStats.getLastValue());
                                else {
                                    DataPointList lastDPSet = tsEngine.periodQuery(object, metric, lastTime, lastTime, 1);
                                    if (lastDPSet.hasNext()) {
                                        jsonDataPoint.put(ServerUtils.Key.OUT_VALUE, lastDPSet.next().getValue());
                                    } else {
                                        throw new TimeseriesException(objectKey+" contains last timestamp but data point not found");
                                    }
                                }
                                jsonResult.put(objectKey, jsonDataPoint);
                            }
                            case ServerUtils.Value.METRIC_QUERY_TYPE_SUM -> {
                                Statistics stats = tsEngine.aggregativeQuery(object, metric, beginTime, endTime);
                                if (stats instanceof NumericStatistics numStats) {
                                    jsonResult.put(objectKey, numStats.getSum());
                                } else {
                                    throw new DataTypeMismatchException(objectKey+" is not numeric");
                                }
                            }
                            case ServerUtils.Value.METRIC_QUERY_TYPE_AVERAGE -> {
                                Statistics stats = tsEngine.aggregativeQuery(object, metric, beginTime, endTime);
                                if (stats instanceof NumericStatistics numStats) {
                                    jsonResult.put(objectKey, numStats.getSum().doubleValue() / numStats.count);
                                } else {
                                    throw new DataTypeMismatchException(objectKey+" is not numeric");
                                }
                            }
                            case ServerUtils.Value.METRIC_QUERY_TYPE_MAX -> {
                                Statistics stats = tsEngine.aggregativeQuery(object, metric, beginTime, endTime);
                                if (stats instanceof NumericStatistics numStats) {
                                    jsonResult.put(objectKey, numStats.getMaxValue());
                                } else {
                                    throw new DataTypeMismatchException(objectKey+" is not numeric");
                                }
                            }
                            case ServerUtils.Value.METRIC_QUERY_TYPE_MIN -> {
                                Statistics stats = tsEngine.aggregativeQuery(object, metric, beginTime, endTime);
                                if (stats instanceof NumericStatistics numStats) {
                                    jsonResult.put(objectKey, numStats.getMinValue());
                                } else {
                                    throw new DataTypeMismatchException(objectKey+" is not numeric");
                                }
                            }
                            default -> throw new MessageParsingException("unsupported single query type '" + queryType + "'");
                        }
                    }catch (TargetNotFoundException e){
                        jsonResult.put(objectKey, null);
                    }
                }
            }
        }
        JSONObject jsonRet = new JSONObject();
        jsonRet.put(ServerUtils.Key.OUT_SUCCESS, true);
        jsonRet.put(ServerUtils.Key.OUT_RESULT, jsonResult);
        return jsonRet;
    }

    private JSONObject getMultipleQueryResult(Database database, Set<Vertex> objects, String queryType, String metric, JSONObject jsonQuery) throws TimeseriesException {
        NestEngine tsEngine = NestEngine.getInstance(database);
        JSONObject jsonRet = new JSONObject();
        jsonRet.put(ServerUtils.Key.OUT_SUCCESS, true);

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
            throw new MessageParsingException("begin time " + beginTime + " larger than end time " + endTime);

        switch (queryType) {
            case ServerUtils.Value.METRIC_QUERY_TYPE_COUNT -> {
                long totalCount = 0;
                for (Vertex object : objects) {
                    totalCount += tsEngine.aggregativeQuery(object, metric, beginTime, endTime).count;
                }
                jsonRet.put(ServerUtils.Key.OUT_RESULT, totalCount);
            }
            case ServerUtils.Value.METRIC_QUERY_TYPE_SUM -> {
                long longSum = 0;
                double doubleSum = 0;
                boolean containDouble = false;

                for (Vertex object : objects) {
                    Statistics stats = tsEngine.aggregativeQuery(object, metric, beginTime, endTime);
                    if (stats instanceof NumericStatistics numStats) {
                        Number sum = numStats.getSum();
                        if (!containDouble && numStats instanceof DoubleStatistics) {
                            doubleSum = longSum;
                            containDouble = true;
                        }
                        if (containDouble)
                            doubleSum += sum.doubleValue();
                        else
                            longSum += sum.longValue();
                    } else {
                        throw new DataTypeMismatchException(String.format("%s:%s.%s is not numeric", object.getTypeName(), object.getString(DatabaseUtils.PROP_OBJECT_ID), metric));
                    }
                }
                if (containDouble)
                    jsonRet.put(ServerUtils.Key.OUT_RESULT, doubleSum);
                else
                    jsonRet.put(ServerUtils.Key.OUT_RESULT, longSum);
            }
            case ServerUtils.Value.METRIC_QUERY_TYPE_AVERAGE -> {
                long totalCount = 0;
                double doubleSum = 0;

                for (Vertex object : objects) {
                    Statistics stats = tsEngine.aggregativeQuery(object, metric, beginTime, endTime);
                    if (stats instanceof NumericStatistics numStats) {
                        doubleSum += numStats.getSum().doubleValue();
                        totalCount += numStats.count;
                    } else {
                        throw new DataTypeMismatchException(String.format("%s:%s.%s is not numeric", object.getTypeName(), object.getString(DatabaseUtils.PROP_OBJECT_ID), metric));
                    }
                }
                jsonRet.put(ServerUtils.Key.OUT_RESULT, doubleSum / totalCount);
            }
            case ServerUtils.Value.METRIC_QUERY_TYPE_MAX -> {
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
                jsonRet.put(ServerUtils.Key.OUT_RESULT, totalMax);
            }
            case ServerUtils.Value.METRIC_QUERY_TYPE_MIN -> {
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
                jsonRet.put(ServerUtils.Key.OUT_RESULT, totalMin);
            }
            default -> throw new MessageParsingException("unsupported multiple query type '" + queryType + "'");
        }
        return jsonRet;
    }
}
