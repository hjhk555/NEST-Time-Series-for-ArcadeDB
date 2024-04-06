package nju.hjh.arcadedb.timeseries;

import com.arcadedb.database.Binary;
import com.arcadedb.database.Database;
import com.arcadedb.database.RID;
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

public class NestEngine {
    public static final String PREFIX_METRIC = "_m";
    public static final String PREFIX_NODE_TYPE = "_n";
    public static final int STATS_NODE_BUCKETS = 1;

    public final Database database;
    public final HashMap<RID, NestNodeRoot> rootCache = new HashMap<>();

    // created instances of TimeseriesEngine
    public static final HashMap<Database, NestEngine> engineInstances = new HashMap<>();

    public NestEngine(Database database) {
        this.database = database;
    }

    public static NestEngine getInstance(Database database){
        synchronized (engineInstances) {
            NestEngine engine = engineInstances.get(database);
            if (engine != null) {
                // check if engine has same database
                if (engine.database == database) return engine;
            }
            engine = new NestEngine(database);
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

    public NestNodeRoot getStatsTreeRoot(Vertex object, String metric) throws TimeseriesException {
        final String metricRIDField = PREFIX_METRIC + metric;
        final String metricDocumentType = PREFIX_NODE_TYPE+object.getIdentity().getBucketId()+"_"+metric;

        // object's statsBlock document not exist
        if (!database.getSchema().existsType(metricDocumentType))
            throw new TargetNotFoundException("object's statsBlock documentType "+ metricDocumentType+" not exist");

        byte[] metricRidBytes = object.getBinary(metricRIDField);
        if (metricRidBytes == null)
            // no existing metric
            throw new TargetNotFoundException("object has no metric "+metric);

        Binary metricRID = new Binary(metricRidBytes);
        RID rid = new RID(database, metricRID.getInt(), metricRID.getLong());

        NestNodeRoot root = rootCache.get(rid);
        if (root != null) return root;

        root = NestNode.loadRoot(rid, metricDocumentType);
        rootCache.put(rid, root);
        return root;
    }

    public NestNodeRoot getOrNewStatsTreeRoot(MutableVertex object, String metric, DataType dataType, int statsTreeDegree) throws TimeseriesException {
        final String metricRIDField = PREFIX_METRIC + metric;
        final String metricDocumentType = PREFIX_NODE_TYPE + object.getIdentity().getBucketId()+"_"+metric;

        // create object's metric document if not exist
        if (!database.getSchema().existsType(metricDocumentType)) {
            DocumentType statsNodeType = database.getSchema().buildDocumentType().withName(metricDocumentType).withTotalBuckets(STATS_NODE_BUCKETS).create();
            statsNodeType.createProperty(NestNode.PROP_NODE_BINARY, Type.BINARY);
        }

        // get root of statsTree
        NestNodeRoot treeRoot;

        byte[] metricRidBytes = object.getBinary(metricRIDField);
        if (metricRidBytes == null){
            // no existing statsBlockRoot, create one
            treeRoot = NestNode.newNest(database, metricDocumentType, dataType, statsTreeDegree);
            Binary metricRID = new Binary(12);
            metricRID.putInt(treeRoot.document.getIdentity().getBucketId());
            metricRID.putLong(treeRoot.document.getIdentity().getPosition());
            object.set(metricRIDField, metricRID.toByteArray());
            object.save();
            rootCache.put(treeRoot.document.getIdentity(), treeRoot);
        }else{
            Binary metricRID = new Binary(metricRidBytes);
            RID rid = new RID(database, metricRID.getInt(), metricRID.getLong());

            treeRoot = rootCache.get(rid);
            if (treeRoot != null) return treeRoot;

            treeRoot = NestNode.loadRoot(rid, metricDocumentType);
            rootCache.put(rid, treeRoot);
            return treeRoot;
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
        insertDataPoint(object, metric, dataType, dataPoint, strategy, NestNode.DEFAULT_TREE_DEGREE);
    }

    public void insertDataPoint(MutableVertex object, String metric, DataType dataType, DataPoint dataPoint, UpdateStrategy strategy, int statsTreeDegree) throws TimeseriesException {
        NestNodeRoot root = getOrNewStatsTreeRoot(object, metric, dataType, statsTreeDegree);
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
        for(NestNodeRoot root : rootCache.values()){
            root.serializeIfDirty();
        }
        database.commit();
        rootCache.clear();
    }

    public void rollback(){
        rootCache.clear();
        if (database.isTransactionActive())
            database.rollback();
    }

}
