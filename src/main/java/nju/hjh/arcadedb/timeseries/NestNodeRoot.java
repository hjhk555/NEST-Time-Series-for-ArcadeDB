package nju.hjh.arcadedb.timeseries;

import com.arcadedb.database.Binary;
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import nju.hjh.arcadedb.timeseries.datapoint.DataPoint;
import nju.hjh.arcadedb.timeseries.exception.TimeseriesException;
import nju.hjh.arcadedb.timeseries.statistics.Statistics;

public class NestNodeRoot extends NestNodeInternal {
    public static final byte BLOCK_TYPE = 0;

    /**
     * size of stat header without statistics and child list:
     * block type(1B) + degree(4B) + data type(5B) + latestRID(12B) + latestStartTime(8B) + child size(4B)
     */
    public static final int HEADER_WITHOUT_STATS_AND_CHILD = 34;

    public ChildInfo latest;

    public NestNodeRoot(Document document, String documentType, int degree, DataType dataType, long beginTimestamp, long endTimestamp) {
        super(document, documentType, degree, dataType, beginTimestamp, endTimestamp, null);
    }

    public void loadLatestNode() throws TimeseriesException {
        latest.node = NestNode.loadNonRoot(latest.rid, documentType, degree, dataType, latest.beginTime, endTimestamp, null);
    }

    @Override
    public void serializeIfDirty() throws TimeseriesException {
        if (latest.node != null) latest.node.serializeIfDirty();
        if (!dirty) return;
        for(int i=0; i<childCount; i++) if (children[i].node != null) children[i].node.serializeIfDirty();

        int infoSize = HEADER_WITHOUT_STATS_AND_CHILD + degree * (CHILD_SIZE_WITHOUT_STATISTICS + Statistics.maxBytesRequired(dataType));

        MutableDocument modifiedDocument = document.modify();
        Binary binary = new Binary(infoSize, false);
        binary.putByte(BLOCK_TYPE);
        binary.putInt(degree);
        dataType.serialize(binary);
        binary.putInt(latest.rid.getBucketId());
        binary.putLong(latest.rid.getPosition());
        binary.putLong(latest.beginTime);
        binary.putInt(childCount);
        for (int i=0; i<childCount; i++){
            binary.putInt(children[i].rid.getBucketId());
            binary.putLong(children[i].rid.getPosition());
            binary.putLong(children[i].beginTime);
            children[i].statistics.serialize(binary);
        }

        if (binary.size() > infoSize)
            throw new TimeseriesException("stat header size exceeded");

        binary.size(infoSize);
        modifiedDocument.set(PROP_NODE_BINARY, binary.toByteArray());
        modifiedDocument.save();
    }

    private final SplitCallback rootSplitCallback = newNode -> {
        // root split, clone root to internal
        MutableDocument newDoc = document.getDatabase().newDocument(documentType);
        NestNodeInternal newInternal = new NestNodeInternal(newDoc, documentType, degree, dataType, beginTimestamp, endTimestamp, Statistics.newEmptyStats(dataType));
        newInternal.dirty = true;

        for (int i = 0; i < childCount; i++) {
            newInternal.children[i] = this.children[i];
            newInternal.statistics.merge(children[i].statistics);
        }
        newInternal.childCount = this.childCount;
        newInternal.serializeIfDirty();

        // connect two internal to root
        childCount = 2;
        children[0] = new ChildInfo(newInternal.document.getIdentity(), newInternal.beginTimestamp, newInternal.statistics, newInternal);
        children[1] = new ChildInfo(newNode.document.getIdentity(), newNode.beginTimestamp, newNode.statistics, newNode);

        // recovery root info
        endTimestamp = MAX_TIMESTAMP;

        dirty = true;
    };

    public void insert(DataPoint data, UpdateStrategy strategy) throws TimeseriesException {
        if (data.timestamp >= latest.beginTime)
            insertLatest(data, strategy);
        else
            super.insert(data, strategy, null, null, rootSplitCallback);
    }

    private void insertLatest(DataPoint data, UpdateStrategy strategy) throws TimeseriesException{
        if (latest.node == null) latest.node = NestNode.loadNonRoot(latest.rid, documentType, degree, dataType, latest.beginTime, MAX_TIMESTAMP, null);
        latest.node.insert(data, strategy, null, null, newNode -> {
            NestNodeLeaf oldLeaf = (NestNodeLeaf) latest.node;
            // insert older node into tree
            if (oldLeaf.statistics == null){
                oldLeaf.statistics = Statistics.newEmptyStats(dataType);
                oldLeaf.statistics.insertAll(oldLeaf.datapoints);
            }
            insertLeafToTree(oldLeaf, null, null, rootSplitCallback);

            // set new latest node
            latest = new ChildInfo(newNode.document.getIdentity(), newNode.beginTimestamp, null, newNode);
            dirty = true;
        });
    }

    @Override
    public Statistics aggregativeQuery(long startTime, long endTime) throws TimeseriesException {
        Statistics result = Statistics.newEmptyStats(dataType);
        if (startTime < latest.beginTime) result.merge(super.aggregativeQuery(startTime, endTime));
        // search in latest
        if (endTime >= latest.beginTime){
            if (latest.node == null) loadLatestNode();
            result.merge(latest.node.aggregativeQuery(startTime, endTime));
        }
        return result;
    }

    @Override
    public DataPointList periodQuery(long startTime, long endTime, int limit) throws TimeseriesException {
        // period start from latest block
        if (startTime >= latest.beginTime) {
            if (latest.node == null) loadLatestNode();
            return latest.node.periodQuery(startTime, endTime, limit);
        }
        return super.periodQuery(startTime, endTime, limit);
    }
}
