package nju.hjh.arcadedb.timeseries.btrdb;

import com.arcadedb.database.Binary;
import com.arcadedb.database.Database;
import com.arcadedb.database.Document;
import com.arcadedb.database.RID;
import nju.hjh.arcadedb.timeseries.DataPointList;
import nju.hjh.arcadedb.timeseries.exception.TimeseriesException;
import nju.hjh.arcadedb.timeseries.btrdb.DoubleStatistics;

public abstract class BtrdbNode {
    public static final String PREFIX_BTRDB_NODE = "_b";
    public static final String PROP_NODE_BINARY = "bin";
    public static final int LEAF_THRESHOLD = 1024;
    public static final int BTRDB_DEGREE = 64;
    public final long begin, end;
    public final Database database;
    public final String metric;
    public DoubleStatistics statistics;
    public boolean dirty = false;

    public static BtrdbRoot newBtrdb(Database database, String metric){
        BtrdbLeaf leaf = new BtrdbLeaf(0, Long.MAX_VALUE, database, metric, null);
        leaf.setAsDirty();
        return new BtrdbRoot(leaf);
    }

    public static BtrdbRoot getBtrdbRoot(Database database, RID rid, String metric, DoubleStatistics statistics) throws TimeseriesException {
        return new BtrdbRoot(getBtrdbNode(database, rid, metric, statistics));
    }

    public static BtrdbNode getBtrdbNode(Database database, RID rid, String metric, DoubleStatistics statistics) throws TimeseriesException {
        Document doc = database.lookupByRID(rid, true).asDocument();
        Binary binary = new Binary(doc.getBinary(PROP_NODE_BINARY));
        switch (binary.getByte()){
            case BtrdbInternal.NODE_TYPE -> {
                BtrdbInternal internal = new BtrdbInternal(binary.getLong(), binary.getLong(), database, metric, statistics);
                for (int i=0; i<BTRDB_DEGREE; i++){
                    RID childRID = new RID(database, binary.getInt(), binary.getLong());
                    if (!childRID.isValid()) continue;
                    DoubleStatistics newStatistics = new DoubleStatistics();
                    newStatistics.deserialize(binary);
                    internal.children[i] = new BtrdbInternal.Child(null, newStatistics, childRID);
                }
                return internal;
            }
            case BtrdbLeaf.NODE_TYPE -> {
                BtrdbLeaf leaf = new BtrdbLeaf(binary.getLong(), binary.getLong(), database, metric, statistics);
                int size = binary.getInt();
                for(int i=0; i<size; i++){
                    leaf.datapoints.put(binary.getLong(), Double.longBitsToDouble(binary.getLong()));
                }
                return leaf;
            }
            default -> {
                throw new TimeseriesException("target not a btrdb node");
            }
        }
    }

    protected BtrdbNode(long begin, long end, Database database, String metric, DoubleStatistics statistics) {
        this.begin = begin;
        this.end = end;
        this.database = database;
        this.metric = metric;
        this.statistics = statistics;
    }

    public void setAsDirty(){
        dirty = true;
    }

    public abstract RID serializeIfDirty();

    public interface InsertionCallback{
        public void call(BtrdbNode entry);
    }

    public abstract void insertDatapoint(long timestamp, double value, InsertionCallback callback) throws TimeseriesException;
    public abstract DoubleStatistics aggregationQuery(long startTime, long endTime) throws TimeseriesException;
    public abstract void periodQuery(long startTime, long endTime, int limits, DataPointList list) throws TimeseriesException;
}
