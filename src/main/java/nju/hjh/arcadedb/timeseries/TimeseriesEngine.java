package nju.hjh.arcadedb.timeseries;

import com.arcadedb.database.Binary;
import com.arcadedb.database.Database;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import nju.hjh.arcadedb.timeseries.datapoint.DataPoint;
import nju.hjh.arcadedb.timeseries.exception.DuplicateTimestampException;
import nju.hjh.arcadedb.timeseries.exception.TargetNotFoundException;
import nju.hjh.arcadedb.timeseries.exception.TimeseriesException;
import nju.hjh.arcadedb.timeseries.statistics.Statistics;

import java.util.*;

public class TimeseriesEngine {
    public static final String PREFIX_METRIC = "_m";
    public static final int STATS_NODE_BUCKETS = 1;
    public final Database database;
    public final ArcadeDocumentManager manager;

    // created instances of TimeseriesEngine
    public static final HashMap<Database, TimeseriesEngine> engineInstances = new HashMap<>();

    public TimeseriesEngine(Database database) {
        this.database = database;
        this.manager = ArcadeDocumentManager.getInstance(database);
    }

    public static TimeseriesEngine getInstance(Database database){
        synchronized (engineInstances) {
            TimeseriesEngine engine = engineInstances.get(database);
            if (engine != null) {
                // check if engine has same database
                if (engine.database == database) return engine;
            }
            engine = new TimeseriesEngine(database);
            engineInstances.put(database, engine);
            return engine;
        }
    }

    public Set<String> getAllMetrics(Vertex object){
        Set<String> res = new HashSet<>();
        for (String propertyName : object.getPropertyNames()){
            if (propertyName.startsWith(PREFIX_METRIC))
                res.add(propertyName.substring(PREFIX_METRIC.length()));
        }
        return res;
    }

    public StatsNodeRoot getStatsTreeRoot(Vertex object, String metric) throws TimeseriesException {
        final String metricRIDField = PREFIX_METRIC + metric;
        final String metricDocumentType = object.getIdentity().getBucketId()+"_"+metric;

        // object's statsBlock document not exist
        if (!database.getSchema().existsType(StatsNode.PREFIX_STATS_NODE +metricDocumentType))
            throw new TargetNotFoundException("object's statsBlock documentType "+ StatsNode.PREFIX_STATS_NODE +metricDocumentType+" not exist");

        byte[] metricRidBytes = object.getBinary(metricRIDField);
        if (metricRidBytes == null)
            // no existing metric
            throw new TargetNotFoundException("object has no metric "+metric);

        Binary metricRID = new Binary(metricRidBytes);
        return StatsNode.getStatsBlockRoot(manager, manager.getRID(metricRID.getInt(), metricRID.getLong()), metricDocumentType);
    }

    public StatsNodeRoot getOrNewStatsTreeRoot(MutableVertex object, String metric, DataType dataType, int statsTreeDegree) throws TimeseriesException {
        final String metricRIDField = PREFIX_METRIC + metric;
        final String metricDocumentType = object.getIdentity().getBucketId()+"_"+metric;

        // create object's metric document if not exist
        if (!database.getSchema().existsType(StatsNode.PREFIX_STATS_NODE +metricDocumentType)) {
            DocumentType statsNodeType = database.getSchema().createDocumentType(StatsNode.PREFIX_STATS_NODE + metricDocumentType, STATS_NODE_BUCKETS);
            statsNodeType.createProperty(StatsNode.PROP_NODE_INFO, Type.BINARY);
        }
        if (!database.getSchema().existsType(StatsNode.PREFIX_DATA_NODE +metricDocumentType)) {
            DocumentType dataNodeType = database.getSchema().createDocumentType(StatsNode.PREFIX_DATA_NODE + metricDocumentType, STATS_NODE_BUCKETS);
            dataNodeType.createProperty(StatsNode.PROP_NODE_INFO, Type.BINARY);
            dataNodeType.createProperty(StatsNode.PROP_NODE_DATA, Type.BINARY);
        }

        // get root of statsTree
        StatsNodeRoot treeRoot;

        byte[] metricRidBytes = object.getBinary(metricRIDField);
        if (metricRidBytes == null){
            // no existing statsBlockRoot, create one
            treeRoot = StatsNode.newStatsTree(manager, metricDocumentType, dataType, statsTreeDegree);
            Binary metricRID = new Binary(32);
            metricRID.putInt(treeRoot.document.getIdentity().getBucketId());
            metricRID.putLong(treeRoot.document.getIdentity().getPosition());
            object.set(metricRIDField, metricRID.toByteArray());
            object.save();
        }else{
            Binary metricRID = new Binary(metricRidBytes);
            treeRoot = StatsNode.getStatsBlockRoot(manager, manager.getRID(metricRID.getInt(), metricRID.getLong()), metricDocumentType);
        }

        return treeRoot;
    }

    /**
     *
     * @param object object to insert data point
     * @param metric name of metric
     * @param dataType type of data
     * @param dataPoint data point to insert
     * @param strategy update strategy if data point exist at target timestamp
     * @throws DuplicateTimestampException if <code>strategy</code> is ERROR and data point already exist at target timestamp
     */
    public void insertDataPoint(MutableVertex object, String metric, DataType dataType, DataPoint dataPoint, UpdateStrategy strategy) throws TimeseriesException {
        insertDataPoint(object, metric, dataType, dataPoint, strategy, StatsNode.DEFAULT_TREE_DEGREE);
    }

    public void insertDataPoint(MutableVertex object, String metric, DataType dataType, DataPoint dataPoint, UpdateStrategy strategy, int statsTreeDegree) throws TimeseriesException {
        StatsNodeRoot root = getOrNewStatsTreeRoot(object, metric, dataType, statsTreeDegree);
        dataPoint = root.dataType.checkAndConvertDataPoint(dataPoint);
        root.insert(dataPoint, strategy);
    }

    public Statistics aggregativeQuery(Vertex object, String metric, long startTime, long endTime) throws TimeseriesException {
        return getStatsTreeRoot(object, metric).aggregativeQuery(startTime, endTime);
    }

    public DataPointList periodQuery(Vertex object, String metric, long startTime, long endTime) throws TimeseriesException {
        return periodQuery(object, metric, startTime, endTime, -1);
    }

    public DataPointList periodQuery(Vertex object, String metric, long startTime, long endTime, int limit) throws TimeseriesException {
        return getStatsTreeRoot(object, metric).periodQuery(startTime, endTime, limit);
    }

    public void begin(){
        database.begin();
        database.setAsyncFlush(false);
    }

    public void commit() throws TimeseriesException {
        manager.saveAll();
        database.commit();
        manager.clearCache();
    }

    public void rollback(){
        manager.clearCache();
        if (database.isTransactionActive())
            database.rollback();
    }

}
