package nju.hjh.arcadedb.timeseries;

import com.arcadedb.database.Binary;
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import nju.hjh.arcadedb.timeseries.datapoint.DataPoint;
import nju.hjh.arcadedb.timeseries.exception.TimeseriesException;
import nju.hjh.arcadedb.timeseries.statistics.Statistics;

public class NestNodeInternal extends NestNode {
    public static final byte BLOCK_TYPE = 1;

    /**
     * size of stat header without statistics and child list:
     * block type(1B) + child size(4B)
     */
    public static final int HEADER_WITHOUT_STATS_AND_CHILD = 5;

    public int childCount = 0;
    public final ChildInfo[] children;

    public NestNodeInternal(Document document, String documentType, int degree, DataType dataType, long beginTimestamp, long endTimestamp, Statistics statistics) {
        super(document, documentType, degree, dataType, beginTimestamp, endTimestamp, statistics);
        children = new ChildInfo[degree+1];
    }

    public void loadChildNode(int index) throws TimeseriesException {
        if (index >= childCount) throw new TimeseriesException("index "+index+" out of bound of "+childCount);
        ChildInfo child = children[index];
        child.node = NestNode.loadNonRoot(child.rid, documentType, degree, dataType, child.beginTime, index == childCount-1 ? endTimestamp : children[index+1].beginTime -1, child.statistics);
    }

    @Override
    public void serializeIfDirty() throws TimeseriesException {
        if (!dirty) return;
        for(int i=0; i<childCount; i++) if (children[i].node != null) children[i].node.serializeIfDirty();
        int infoSize = HEADER_WITHOUT_STATS_AND_CHILD + degree * (CHILD_SIZE_WITHOUT_STATISTICS + Statistics.maxBytesRequired(dataType));

        MutableDocument mutableDocument = document.modify();
        Binary binary = new Binary(infoSize, false);
        binary.putByte(BLOCK_TYPE);
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
        mutableDocument.set(PROP_NODE_BINARY, binary.toByteArray());
        mutableDocument.save();
    }

    @Override
    public void insert(DataPoint data, UpdateStrategy strategy, InsertionCallback insertionCallback, UpdateCallback updateCallback, SplitCallback splitCallback) throws TimeseriesException {
        if (childCount == 0)
            throw new TimeseriesException("cannot insert datapoint as there's no child block");
        if (data.timestamp < beginTimestamp || data.timestamp > endTimestamp)
            throw new TimeseriesException("cannot insert datapoint into tree as no child block can manage target timestamp");

        int insertPos = MathUtils.longBinarySearchFormer(children, 0, childCount, data.timestamp, child -> child.beginTime);
        ChildInfo insertChild = children[insertPos];
        if (insertChild.node == null) loadChildNode(insertPos);
        insertChild.node.insert(data, strategy, newDP -> {
            dirty = true;
            if (statistics != null) statistics.insert(newDP);
            if (insertionCallback != null) insertionCallback.call(newDP);
        }, (oldDP, newDP) -> {
            dirty = true;
            if (statistics != null && !statistics.update(oldDP, newDP)){
                // re calc statistic
                statistics.clear();
                for(int i=0; i<childCount; i++) statistics.merge(children[i].statistics);
            }
            if (updateCallback != null) updateCallback.call(oldDP, newDP);
        }, newNode -> {
            addChild(newNode, splitCallback);
        });
    }

    public void addChild(NestNode newChild, SplitCallback splitCallback) throws TimeseriesException {
        int insertPos = MathUtils.longBinarySearchLatter(children, 0, childCount, newChild.beginTimestamp, child -> child.beginTime);

        for(int i=childCount; i>insertPos; i--) children[i] = children[i-1];
        children[insertPos] = new ChildInfo(newChild.document.getIdentity(), newChild.beginTimestamp, newChild.statistics, newChild);
        childCount++;

        // check split need, childCount = degree+1
        if (childCount > degree){
            int splitCount = childCount*(endTimestamp == MAX_TIMESTAMP ? LATEST_SPLIT_RATIO : OLD_SPLIT_RATIO)/100;
            // create new node
            MutableDocument newDoc = document.getDatabase().newDocument(documentType);
            NestNodeInternal newInternal = new NestNodeInternal(newDoc, documentType, degree, dataType, children[splitCount].beginTime, endTimestamp, Statistics.newEmptyStats(dataType));
            newInternal.dirty = true;
            for(int i=splitCount; i<childCount; i++){
                newInternal.children[i-splitCount] = children[i];
                newInternal.statistics.merge(children[i].statistics);
            }
            newInternal.childCount = childCount-splitCount;
            endTimestamp = children[splitCount].beginTime -1;
            childCount = splitCount;
            if (statistics != null){
                statistics.clear();;
                for(int i=0; i<childCount; i++) statistics.merge(children[i].statistics);
            }
            newInternal.serializeIfDirty();
            if (splitCallback != null) splitCallback.call(newInternal);
        }
        dirty = true;
    }

    @Override
    public void insertLeafToTree(NestNodeLeaf leaf, BatchInsertionCallback batchInsertionCallback, ReachLeafCallback reachLeafCallback, SplitCallback splitCallback) throws TimeseriesException {
        if (leaf.beginTimestamp < this.beginTimestamp || leaf.endTimestamp > this.endTimestamp)
            throw new TimeseriesException("leaf node out of range");
        if (childCount == 0){
            // empty tree, insert into root
            addChild(leaf, splitCallback);
            return;
        }

        int insertPos = MathUtils.longBinarySearchFormer(children, 0, childCount, leaf.beginTimestamp, child -> child.beginTime);
        ChildInfo insertChild = children[insertPos];
        if (insertChild.node == null) loadChildNode(insertPos);
        insertChild.node.insertLeafToTree(leaf, newStatistics -> {
            dirty = true;
            if (statistics != null) statistics.merge(newStatistics);
            if (batchInsertionCallback != null) batchInsertionCallback.call(newStatistics);
        }, () -> {
            addChild(leaf, splitCallback);
        }, newNode -> {
            addChild(newNode, splitCallback);
        });
    }

    @Override
    public Statistics aggregativeQuery(long startTime, long endTime) throws TimeseriesException {
        // locate first block
        int pos = MathUtils.longBinarySearchFormer(children, 0, childCount, startTime, child -> child.beginTime);
        // check if start from first
        if (pos == -1) pos = 0;
        Statistics resultStats = Statistics.newEmptyStats(dataType);
        for(; pos<childCount; pos++){
            ChildInfo cur = children[pos];
            // break if over search
            if (cur.beginTime > endTime) break;
            if (startTime <= cur.statistics.firstTime && endTime >= cur.statistics.lastTime){
                // direct merge
                resultStats.merge(cur.statistics);
            }else{
                // deep search
                if (cur.node == null) loadChildNode(pos);
                resultStats.merge(cur.node.aggregativeQuery(startTime, endTime));
            }
        }
        return resultStats;
    }

    @Override
    public DataPointList periodQuery(long startTime, long endTime, int limit) throws TimeseriesException {
        // locate first block
        int queryPos = MathUtils.longBinarySearchFormer(children, 0, childCount, startTime, child -> child.beginTime);
        ChildInfo queryChild = children[queryPos];
        if (queryChild.node == null) loadChildNode(queryPos);
        return queryChild.node.periodQuery(startTime, endTime, limit);
    }
}
