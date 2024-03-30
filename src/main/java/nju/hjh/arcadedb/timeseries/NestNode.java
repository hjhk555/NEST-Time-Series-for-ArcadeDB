package nju.hjh.arcadedb.timeseries;

import com.arcadedb.database.*;
import nju.hjh.arcadedb.timeseries.datapoint.DataPoint;
import nju.hjh.arcadedb.timeseries.exception.TimeseriesException;
import nju.hjh.arcadedb.timeseries.statistics.Statistics;

import java.util.TreeMap;

public abstract class NestNode{
    public static final int DEFAULT_TREE_DEGREE = 32;
    public static final int MAX_DATA_BLOCK_SIZE = 4096;
    public static final long MAX_TIMESTAMP = Long.MAX_VALUE;
    public static final int LATEST_SPLIT_RATIO = 90;
    public static final int OLD_SPLIT_RATIO = 50;
    public static final RID NULL_RID = new RID(null, -1, -1);
    public static final String PROP_NODE_BINARY = "bin";

    public static class ChildInfo{
        public RID rid;
        public final Statistics statistics;
        public long beginTime;
        public NestNode node;

        public ChildInfo(RID rid, long beginTime, Statistics statistics, NestNode node) {
            this.rid = rid;
            this.statistics = statistics;
            this.beginTime = beginTime;
            this.node = node;
        }
    }

    /**
     * size of one child block
     * childRID(12B) + childStartTime(8B)
     */
    public static final int CHILD_SIZE_WITHOUT_STATISTICS = 20;

    public final Document document;
    public final String documentType;
    public final int degree;
    public final DataType dataType;
    public final long beginTimestamp;
    public long endTimestamp;
    public Statistics statistics;
    public boolean dirty = false;

    public NestNode(Document document, String documentType, int degree, DataType dataType, long beginTimestamp, long endTimestamp, Statistics statistics){
        this.document = document;
        this.documentType = documentType;
        this.degree = degree;
        this.dataType = dataType;
        this.beginTimestamp = beginTimestamp;
        this.endTimestamp = endTimestamp;
        this.statistics = statistics;
    }

    public static NestNodeRoot loadRoot(RID rid, String documentType) throws TimeseriesException {
        Database database = (Database) rid.getDatabase();
        Document document = database.lookupByRID(rid, true).asDocument();
        Binary binary = new Binary(document.getBinary(PROP_NODE_BINARY));
        if (binary.getByte() != NestNodeRoot.BLOCK_TYPE) throw new TimeseriesException("non-root node found when loading root node");
        int degree = binary.getInt();
        DataType dataType = DataType.resolveFromBinary(binary);

        NestNodeRoot root = new NestNodeRoot(document, documentType, degree, dataType, 0L, MAX_TIMESTAMP);
        root.latest = new ChildInfo(new RID(database, binary.getInt(), binary.getLong()), binary.getLong(), null, null);
        root.childCount = binary.getInt();
        for(int i=0; i<root.childCount; i++)
            root.children[i] = new ChildInfo(new RID(database, binary.getInt(), binary.getLong()), binary.getLong(), Statistics.getStatisticsFromBinary(dataType, binary), null);
        return root;
    }

    public static NestNode loadNonRoot(RID rid, String documentType, int degree, DataType dataType, long beginTimestamp, long endTimestamp, Statistics statistics) throws TimeseriesException {
        Database database = (Database) rid.getDatabase();
        Document document = database.lookupByRID(rid, true).asDocument();
        Binary binary = new Binary(document.getBinary(PROP_NODE_BINARY));
        switch (binary.getByte()) {
            case NestNodeRoot.BLOCK_TYPE -> {
                throw new TimeseriesException("root node found when loading non-root node");
            }
            case NestNodeInternal.BLOCK_TYPE -> {
                NestNodeInternal internal = new NestNodeInternal(document, documentType, degree, dataType, beginTimestamp, endTimestamp, statistics);
                internal.childCount = binary.getInt();
                for(int i=0; i<internal.childCount; i++)
                    internal.children[i] = new ChildInfo(new RID(database, binary.getInt(), binary.getLong()), binary.getLong(), Statistics.getStatisticsFromBinary(dataType, binary), null);
                return internal;
            }
            case NestNodeLeaf.BLOCK_TYPE -> {
                NestNodeLeaf leaf = new NestNodeLeaf(document, documentType, degree, dataType, beginTimestamp, endTimestamp, statistics);
                leaf.succRID = new RID(database, binary.getInt(), binary.getLong());
                int dataSize = binary.getInt();
                leaf.datapoints = new TreeMap<>();
                for(int i=0; i<dataSize; i++){
                    DataPoint newPoint = DataPoint.getDataPointFromBinary(dataType, binary);
                    leaf.datapoints.put(newPoint.timestamp, newPoint);
                }
                return leaf;
            }
            default -> throw new TimeseriesException("node has invalid type");
        }
    }

    public static NestNodeRoot newNest(Database database, String documentType, DataType dataType) throws TimeseriesException {
        return newNest(database, documentType, dataType, DEFAULT_TREE_DEGREE);
    }

    public static NestNodeRoot newNest(Database database, String documentType, DataType dataType, int degree) throws TimeseriesException {
        // root node
        MutableDocument docRoot = database.newDocument(documentType);
        NestNodeRoot newTreeRoot = new NestNodeRoot(docRoot, documentType, degree, dataType, 0, MAX_TIMESTAMP);
        newTreeRoot.dirty = true;

        // leaf node
        MutableDocument docLeaf = database.newDocument(documentType);
        NestNodeLeaf newLeaf = new NestNodeLeaf(docLeaf, documentType, degree, dataType, 0, MAX_TIMESTAMP, null);
        newLeaf.dirty = true;

        newLeaf.datapoints = new TreeMap<>();
        newLeaf.succRID = NULL_RID;
        newLeaf.serializeIfDirty();

        // link leaf to root
        newTreeRoot.latest = new ChildInfo(newLeaf.document.getIdentity(), 0, null, newLeaf);
        newTreeRoot.serializeIfDirty();

        return newTreeRoot;
    }

    public interface InsertionCallback{
        public void call(DataPoint newDP) throws TimeseriesException;
    }
    public interface UpdateCallback{
        public void call(DataPoint oldDP, DataPoint newDP) throws TimeseriesException;
    }
    public interface ReachLeafCallback{
        public void call() throws TimeseriesException;
    }
    public interface BatchInsertionCallback{
        public void call(Statistics statistics) throws TimeseriesException;
    }
    public interface SplitCallback{
        public void call(NestNode newNode) throws TimeseriesException;
    }

    /**
     * inesrt data point into statsBlock
     * @param data the data point to insert
     * @throws TimeseriesException
     */
    public abstract void insert(DataPoint data, UpdateStrategy strategy, InsertionCallback insertionCallback, UpdateCallback updateCallback, SplitCallback splitCallback) throws TimeseriesException;

    // insert leaf block into tree
    public abstract void insertLeafToTree(NestNodeLeaf leaf, BatchInsertionCallback batchInsertionCallback, ReachLeafCallback reachLeafCallback, SplitCallback splitCallback) throws TimeseriesException;

    public abstract Statistics aggregativeQuery(long startTime, long endTime) throws TimeseriesException;

    public abstract DataPointList periodQuery(long startTime, long endTime, int limit) throws TimeseriesException;

    public abstract void serializeIfDirty() throws TimeseriesException;
}
