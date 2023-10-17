package nju.hjh.arcadedb.timeseries;

import com.arcadedb.database.Database;
import com.arcadedb.graph.Edge;
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
    public static final String PREFIX_METRIC_BUCKETID = "_mb";
    public static final String PREFIX_METRIC_POSITION = "_mp";
    public static final int STATSBLOCK_BUCKETS = 1;
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
            if (propertyName.startsWith(PREFIX_METRIC_BUCKETID))
                res.add(propertyName.substring(PREFIX_METRIC_BUCKETID.length()));
        }
        return res;
    }

    public StatsBlockRoot getStatsTreeRoot(Vertex object, String metric) throws TimeseriesException {
        final String metricBucketIdField = PREFIX_METRIC_BUCKETID + metric;
        final String metricPositionField = PREFIX_METRIC_POSITION + metric;
        final String metricDocumentType = object.getIdentity().getBucketId()+"_"+metric;

        // object's statsBlock document not exist
        if (!database.getSchema().existsType(StatsBlock.PREFIX_STATSBLOCK+metricDocumentType))
            throw new TargetNotFoundException("object's statsBlock documentType "+StatsBlock.PREFIX_STATSBLOCK+metricDocumentType+" not exist");

        Integer metricBucketId = object.getInteger(metricBucketIdField);
        Long metricOffset = object.getLong(metricPositionField);

        // no existing metric
        if (metricBucketId == null || metricOffset == null){
            throw new TargetNotFoundException("object has no metric "+metric);
        }else{
            return StatsBlock.getStatsBlockRoot(manager, manager.getRID(metricBucketId, metricOffset), metricDocumentType);
        }
    }

    public StatsBlockRoot getOrNewStatsTreeRoot(Vertex object, String metric, DataType dataType, int statsTreeDegree) throws TimeseriesException {
        final String metricBucketIdField = PREFIX_METRIC_BUCKETID + metric;
        final String metricPositionField = PREFIX_METRIC_POSITION + metric;
        final String metricDocumentType = object.getIdentity().getBucketId()+"_"+metric;

        // create object's metric document if not exist
        if (!database.getSchema().existsType(StatsBlock.PREFIX_STATSBLOCK+metricDocumentType)) {
            DocumentType statsBlockType = database.getSchema().createDocumentType(StatsBlock.PREFIX_STATSBLOCK + metricDocumentType, STATSBLOCK_BUCKETS);
            statsBlockType.createProperty("stat", Type.BINARY);
            statsBlockType.createProperty("data", Type.BINARY);
        }

        // get root of statsTree
        StatsBlockRoot treeRoot;

        Integer metricBucketId = object.getInteger(metricBucketIdField);
        Long metricOffset = object.getLong(metricPositionField);

        // no existing statsBlockRoot, create one
        if (metricBucketId == null || metricOffset == null){
            treeRoot = StatsBlock.newStatsTree(manager, metricDocumentType, dataType, statsTreeDegree);
            MutableVertex dirtyObject = object.modify();
            dirtyObject.set(metricBucketIdField, treeRoot.document.getIdentity().getBucketId());
            dirtyObject.set(metricPositionField, treeRoot.document.getIdentity().getPosition());
            dirtyObject.save();
        }else{
            treeRoot = StatsBlock.getStatsBlockRoot(manager, manager.getRID(metricBucketId, metricOffset), metricDocumentType);
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
    public void insertDataPoint(Vertex object, String metric, DataType dataType, DataPoint dataPoint, UpdateStrategy strategy) throws TimeseriesException {
        insertDataPoint(object, metric, dataType, dataPoint, strategy, StatsBlock.DEFAULT_TREE_DEGREE);
    }

    public void insertDataPoint(Vertex object, String metric, DataType dataType, DataPoint dataPoint, UpdateStrategy strategy, int statsTreeDegree) throws TimeseriesException {
        StatsBlockRoot root = getOrNewStatsTreeRoot(object, metric, dataType, statsTreeDegree);
        dataPoint = root.dataType.checkAndConvertDataPoint(dataPoint);
        root.insert(dataPoint, strategy);
    }

    public Statistics aggregativeQuery(Vertex object, String metric, long startTime, long endTime) throws TimeseriesException {
        return getStatsTreeRoot(object, metric).aggregativeQuery(startTime, endTime);
    }

    public DataPointSet periodQuery(Vertex object, String metric, long startTime, long endTime) throws TimeseriesException {
        return getStatsTreeRoot(object, metric).periodQuery(startTime, endTime);
    }

    public void begin(){
        database.begin();
    }

    public void commit(){
        manager.saveAll();
        database.commit();
    }

    public void rollback(){
        manager.clearCache();
        database.rollback();
    }

}
