package nju.hjh.arcadedb.timeseries;

import com.arcadedb.database.Binary;
import com.arcadedb.database.Database;
import com.arcadedb.database.RID;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import lombok.Getter;
import nju.hjh.arcadedb.timeseries.datapoint.DataPoint;
import nju.hjh.arcadedb.timeseries.exception.DuplicateTimestampException;
import nju.hjh.arcadedb.timeseries.exception.TargetNotFoundException;
import nju.hjh.arcadedb.timeseries.exception.TimeseriesException;
import nju.hjh.arcadedb.timeseries.statistics.Statistics;
import nju.hjh.arcadedb.timeseries.types.DataType;

import java.util.*;

public class NestEngine {
    public static final String PREFIX_METRIC = "_m";
    public static final String PREFIX_NODE_TYPE = "_n";
    public static final int STATS_NODE_BUCKETS = 1;

    @Getter
    private final Database database;
    private final HashMap<RID, NestNodeRoot> rootCache = new HashMap<>();

    public NestEngine(Database database) {
        this.database = database;
    }

    public Set<String> getAllMetrics(Vertex object){
        Set<String> res = new HashSet<>();
        for (String propertyName : object.getPropertyNames()){
            if (propertyName.startsWith(PREFIX_METRIC))
                res.add(propertyName.substring(PREFIX_METRIC.length()));
        }
        return res;
    }

    private NestNodeRoot getStatsTreeRoot(Vertex object, String metric) throws TimeseriesException {
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

    public void createIfAbsentStatsTree(MutableVertex object, String metric, DataType type) throws TimeseriesException {
        createIfAbsentStatsTree(object, metric, type, NestNode.DEFAULT_TREE_DEGREE);
    }

    public void createIfAbsentStatsTree(MutableVertex object, String metric, DataType type, int degree) throws TimeseriesException {
        final String metricRIDField = PREFIX_METRIC + metric;
        final String metricDocumentType = PREFIX_NODE_TYPE + object.getIdentity().getBucketId()+"_"+metric;

        // create object's metric document if not exist
        if (!database.getSchema().existsType(metricDocumentType)) {
            DocumentType statsNodeType = database.getSchema().buildDocumentType().withName(metricDocumentType).withTotalBuckets(STATS_NODE_BUCKETS).create();
            statsNodeType.createProperty(NestNode.PROP_NODE_BINARY, Type.BINARY);
        }

        byte[] metricRidBytes = object.getBinary(metricRIDField);
        if (metricRidBytes != null) return; // exists

        // no existing statsBlockRoot, create one
        NestNodeRoot treeRoot = NestNode.newNest(database, metricDocumentType, type, degree);
        Binary metricRID = new Binary(12);
        metricRID.putInt(treeRoot.document.getIdentity().getBucketId());
        metricRID.putLong(treeRoot.document.getIdentity().getPosition());
        object.set(metricRIDField, metricRID.toByteArray());
        object.save();
        rootCache.put(treeRoot.document.getIdentity(), treeRoot);
    }

    /**
     *
     * @param object object to insert data point
     * @param metric name of metric
     * @param timestamp timestamp of datapoint
     * @param value value of datapoint
     * @param strategy update strategy if data point exist at target timestamp
     * @throws DuplicateTimestampException if <code>strategy</code> is ERROR and data point already exist at target timestamp
     */
    public void insertDataPoint(MutableVertex object, String metric, long timestamp, Object value, UpdateStrategy strategy) throws TimeseriesException {
        insertDataPoint(object, metric, timestamp, value, strategy, NestNode.DEFAULT_TREE_DEGREE);
    }

    public void insertDataPoint(MutableVertex object, String metric, long timestamp, Object value, UpdateStrategy strategy, int statsTreeDegree) throws TimeseriesException {
        createIfAbsentStatsTree(object, metric, DataType.inferDatatype(value), statsTreeDegree);
        NestNodeRoot root = getStatsTreeRoot(object, metric);
        DataPoint dataPoint = root.dataType.convertValue2DataPoint(timestamp, value);
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
