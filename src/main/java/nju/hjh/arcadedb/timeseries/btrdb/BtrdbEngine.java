package nju.hjh.arcadedb.timeseries.btrdb;

import com.arcadedb.database.*;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import nju.hjh.arcadedb.timeseries.DataPointList;
import nju.hjh.arcadedb.timeseries.exception.TargetNotFoundException;
import nju.hjh.arcadedb.timeseries.exception.TimeseriesException;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

public class BtrdbEngine {
    public static final String PREFIX_METRIC = "_m";
    public static final int STATS_NODE_BUCKETS = 1;
    public final Database database;
    public static class CacheEntry {
        public Vertex vertex;
        public String field;
        public BtrdbRoot root;

        public CacheEntry(Vertex vertex, String field, BtrdbRoot root) {
            this.vertex = vertex;
            this.field = field;
            this.root = root;
        }
    }
    public final HashMap<RID, CacheEntry> cache = new HashMap<>();

    public BtrdbEngine(Database database) {
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

    public BtrdbRoot getBtrdbRoot(Vertex object, String metric) throws TimeseriesException {
        final String metricRIDField = PREFIX_METRIC + metric;
        final String metricDocumentType = BtrdbNode.PREFIX_BTRDB_NODE + object.getIdentity().getBucketId()+"_"+metric;

        // object's statsBlock document not exist
        if (!database.getSchema().existsType(metricDocumentType))
            throw new TargetNotFoundException("object's btrdb documentType "+metricDocumentType+" not exist");

        byte[] metricRidBytes = object.getBinary(metricRIDField);
        if (metricRidBytes == null)
            // no existing metric
            throw new TargetNotFoundException("object has no metric "+metric);

        Binary metricRID = new Binary(metricRidBytes);
        RID rid = new RID(database, metricRID.getInt(), metricRID.getLong());
        CacheEntry entry = cache.get(rid);
        if (entry != null) return entry.root;
        // access database
        BtrdbRoot root = BtrdbNode.getBtrdbRoot(database, rid, metricRIDField, null);
        cache.put(rid, new CacheEntry(object, metricRIDField, root));
        return root;
    }

    public BtrdbRoot getOrNewBtrdbRoot(MutableVertex object, String metric) throws TimeseriesException {
        final String metricRIDField = PREFIX_METRIC + metric;
        final String metricDocumentType = BtrdbNode.PREFIX_BTRDB_NODE + object.getIdentity().getBucketId()+"_"+metric;

        // create object's metric document if not exist
        if (!database.getSchema().existsType(metricDocumentType)) {
            DocumentType statsNodeType = database.getSchema().buildDocumentType().withName(metricDocumentType).withTotalBuckets(STATS_NODE_BUCKETS).create();
            statsNodeType.createProperty(BtrdbNode.PROP_NODE_BINARY, Type.BINARY);
        }

        // get root of statsTree
        BtrdbRoot root;

        byte[] metricRidBytes = object.getBinary(metricRIDField);
        if (metricRidBytes == null){
            // no existing statsBlockRoot, create one
            root = BtrdbNode.newBtrdb(database, metricDocumentType);
            RID newRID = root.commit();
            Binary binary = new Binary();
            binary.putInt(newRID.getBucketId());
            binary.putLong(newRID.getPosition());
            object.set(metricRIDField, binary.toByteArray());
            object.save();
            cache.put(newRID, new CacheEntry(object, metricRIDField, root));
        }else{
            Binary metricRID = new Binary(metricRidBytes);
            RID rid = new RID(database, metricRID.getInt(), metricRID.getLong());
            CacheEntry entry = cache.get(rid);
            if (entry != null) {
                entry.vertex = object;
                return entry.root;
            }
            root = BtrdbNode.getBtrdbRoot(database, rid, metricDocumentType, null);
            cache.put(rid, new CacheEntry(object, metricRIDField, root));
        }
        return root;
    }

    public void insertDataPoint(MutableVertex object, String metric, long timestamp, double value) throws TimeseriesException {
        getOrNewBtrdbRoot(object, metric).insertDatapoint(timestamp, value);
    }


    public DoubleStatistics aggregativeQuery(Vertex object, String metric, long startTime, long endTime) throws TimeseriesException {
        return getBtrdbRoot(object, metric).aggregationQuery(startTime, endTime);
    }

    public DataPointList periodQuery(Vertex object, String metric, long startTime, long endTime) throws TimeseriesException {
        return periodQuery(object, metric, startTime, endTime, -1);
    }

    public DataPointList periodQuery(Vertex object, String metric, long startTime, long endTime, int limit) throws TimeseriesException {
        return getBtrdbRoot(object, metric).periodQuery(startTime, endTime, limit);
    }

    public void begin(){
        database.begin();
        database.setAsyncFlush(false);
    }

    public void commit() throws TimeseriesException {
        // commit cache
        for(CacheEntry entry : cache.values()){
            RID newRID = entry.root.commit();
            if (newRID != null){
                Binary ridBinary = new Binary();
                ridBinary.putInt(newRID.getBucketId());
                ridBinary.putLong(newRID.getPosition());
                MutableDocument document = entry.vertex.modify();
                document.set(entry.field, ridBinary.toByteArray());
                document.save();
            }
        }
        database.commit();
        cache.clear();
    }

    public void rollback(){
        cache.clear();
        if (database.isTransactionActive())
            database.rollback();
    }
}
