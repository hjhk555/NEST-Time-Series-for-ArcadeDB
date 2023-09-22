package nju.hjh.arcadedb.timeseries;

import com.arcadedb.database.Binary;
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import nju.hjh.arcadedb.timeseries.datapoint.DataPoint;
import nju.hjh.arcadedb.timeseries.exception.TimeseriesException;
import nju.hjh.arcadedb.timeseries.statistics.Statistics;

import java.util.ArrayList;

public class StatsBlockInternal extends StatsBlock{
    public static final byte BLOCK_TYPE = 1;

    /**
     * size of stat header without statistics and child list:
     * block type(1B) + child size(4B)
     */
    public static final int HEADER_WITHOUT_STATS_AND_CHILD = 5;

    public StatsBlock parent;
    public ArrayList<RID> childRID = new ArrayList<>();
    public ArrayList<Long> childStartTime = new ArrayList<>();

    public StatsBlockInternal(ArcadeDocumentManager manager, Document document, String metric, int degree, DataType dataType) {
        super(manager, document, metric, degree, dataType);
    }

    @Override
    public MutableDocument serializeDocument() throws TimeseriesException {
        int statSize = HEADER_WITHOUT_STATS_AND_CHILD + Statistics.maxBytesRequired(dataType) + degree * CHILD_SIZE;

        MutableDocument mutableDocument = document.modify();
        Binary binary = new Binary(statSize, false);
        binary.putByte(BLOCK_TYPE);
        statistics.serialize(binary);
        binary.putInt(childRID.size());
        for (int i=0; i<childRID.size(); i++){
            binary.putInt(childRID.get(i).getBucketId());
            binary.putLong(childRID.get(i).getPosition());
            binary.putLong(childStartTime.get(i));
        }
        if (binary.size() > statSize)
            throw new TimeseriesException("stat header size exceeded");

        binary.size(statSize);
        mutableDocument.set("stat", binary.toByteArray());
        return mutableDocument;
    }

    @Override
    public void setParent(StatsBlock parent) {
        this.parent = parent;
    }

    @Override
    public void insert(DataPoint data, UpdateStrategy strategy) throws TimeseriesException {
        if (childStartTime.size() == 0)
            throw new TimeseriesException("cannot insert datapoint as there's no child block");

        int insertPos = MathUtils.longBinarySearchFormer(childStartTime, data.timestamp);
        if (insertPos == -1)
            throw new TimeseriesException("cannot insert datapoint into tree as no child block can manage target timestamp");

        StatsBlock blockToInsert = getStatsBlockNonRoot(manager, childRID.get(insertPos), metric, degree, dataType);
        blockToInsert.setParent(this);
        blockToInsert.setStartTime(childStartTime.get(insertPos));
        blockToInsert.setActive(blockToInsert instanceof StatsBlockInternal && this.isActive && (insertPos == childRID.size() - 1));
        blockToInsert.insert(data, strategy);
    }

    @Override
    public void appendStats(DataPoint data) throws TimeseriesException {
        this.statistics.insert(data);
        parent.appendStats(data);
        setAsDirty();
    }

    @Override
    public void updateStats(DataPoint oldDP, DataPoint newDP) throws TimeseriesException {
        if (!this.statistics.update(oldDP, newDP)){
            this.statistics = Statistics.newEmptyStats(dataType);
            for (RID rid : childRID) {
                this.statistics.merge(getStatsBlockNonRoot(manager, rid, metric, degree, dataType).statistics);
            }
        }
        parent.updateStats(oldDP, newDP);
        setAsDirty();
    }

    @Override
    public void addChild(StatsBlock child) throws TimeseriesException {
        int insertPos = MathUtils.longBinarySearchLatter(childStartTime, child.startTime);
        if (insertPos < childStartTime.size() && childStartTime.get(insertPos) == child.startTime)
            throw new TimeseriesException("cannot insert child with existing startTime");

        childRID.add(insertPos, child.document.getIdentity());
        childStartTime.add(insertPos, child.startTime);

        if (childRID.size() > degree){
            // split into 2 blocks
            int totalSize = childRID.size();
            int splitedSize;
            if (isActive)
                splitedSize = degree;
            else
                splitedSize = totalSize / 2;

            StatsBlockInternal newInternal = (StatsBlockInternal) manager.newArcadeDocument(PREFIX_STATSBLOCK + metric, document1 -> {
                return new StatsBlockInternal(manager, document1, metric, degree, dataType);
            });
            newInternal.setStartTime(childStartTime.get(splitedSize));
            newInternal.setActive(isActive);
            this.setActive(false);

            // remake statistics of latter block
            newInternal.childRID = new ArrayList<>(this.childRID.subList(splitedSize, totalSize));
            newInternal.childStartTime = new ArrayList<>(this.childStartTime.subList(splitedSize, totalSize));
            newInternal.statistics = Statistics.newEmptyStats(dataType);
            for (RID rid : newInternal.childRID)
                newInternal.statistics.merge(getStatsBlockNonRoot(manager, rid, metric, degree, dataType).statistics);
            newInternal.save();

            // remake statistics of this block
            childRID = new ArrayList<>(childRID.subList(0, splitedSize));
            childStartTime = new ArrayList<>(childStartTime.subList(0, splitedSize));
            statistics = Statistics.newEmptyStats(dataType);
            for (RID rid : childRID)
                statistics.merge(getStatsBlockNonRoot(manager, rid, metric, degree, dataType).statistics);

            parent.addChild(newInternal);
        }

        setAsDirty();
    }

    @Override
    public void addLeafBlock(StatsBlockLeaf leaf) throws TimeseriesException {
        // merge statistics
        statistics.merge(leaf.statistics);

        // locate insert block
        int insertPos = MathUtils.longBinarySearchFormer(childStartTime, leaf.startTime);
        StatsBlock blockToInsert = StatsBlock.getStatsBlockNonRoot(manager, childRID.get(insertPos), metric, degree, dataType);
        blockToInsert.setStartTime(childStartTime.get(insertPos));
        blockToInsert.setParent(this);
        blockToInsert.setActive(blockToInsert instanceof StatsBlockInternal && isActive && (insertPos == childRID.size() - 1));
        blockToInsert.addLeafBlock(leaf);

        setAsDirty();
    }

    @Override
    public Statistics aggregativeQuery(long startTime, long endTime) throws TimeseriesException {
        // if range out of this block's statistics
        if (startTime > statistics.lastTime || endTime < statistics.firstTime){
            return Statistics.newEmptyStats(dataType);
        }
        // if range covers this block's statistics
        if (startTime <= statistics.firstTime && endTime >= statistics.lastTime){
            return this.statistics.clone();
        }

        // locate first block
        int pos;
        if (childStartTime.get(0) >= startTime)
            // start from head
            pos = 0;
        else
            pos = MathUtils.longBinarySearchFormer(childStartTime, startTime);
        Statistics resultStats = Statistics.newEmptyStats(dataType);

        // merge block's statistics
        while (pos < childRID.size() && childStartTime.get(pos) <= endTime){
            resultStats.merge(getStatsBlockNonRoot(manager, childRID.get(pos), metric, degree, dataType).aggregativeQuery(startTime, endTime));
            pos++;
        }
        return resultStats;
    }

    @Override
    public DataPointSet periodQuery(long startTime, long endTime) throws TimeseriesException {
        // locate first block
        int startPos = MathUtils.longBinarySearchFormer(childStartTime, startTime);
        return getStatsBlockNonRoot(manager, childRID.get(startPos), metric, degree, dataType).periodQuery(startTime, endTime);
    }
}
