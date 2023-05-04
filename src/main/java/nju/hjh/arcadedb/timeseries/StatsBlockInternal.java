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

    public StatsBlockInternal(ArcadeDocumentManager manager, Document document, String measurement, int degree, DataType dataType, long startTime, boolean isLatest) {
        super(manager, document, measurement, degree, dataType, startTime, isLatest);
    }

    @Override
    public MutableDocument serializeDocument() throws TimeseriesException {
        int statSize = HEADER_WITHOUT_STATS_AND_CHILD + Statistics.bytesToWrite(dataType) + degree * CHILD_SIZE;

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
    public void insert(DataPoint data, boolean updateIfExist) throws TimeseriesException {
        if (childStartTime.size() == 0)
            throw new TimeseriesException("cannot insert datapoint as there's no child block");

        int pos = -1;
        if (childStartTime.get(childRID.size()-1) <= data.timestamp){
            // insert into latest
            pos = childRID.size()-1;
        }else {
            // binary search
            int low = 0, high = childRID.size() - 1;
            // for not latest block
            while (low <= high) {
                int mid = (low + high) >>> 1;
                long midStartTime = childStartTime.get(mid);
                if (midStartTime > data.timestamp)
                    high = mid - 1;
                else if (midStartTime < data.timestamp)
                    low = mid + 1;
                else {
                    pos = mid;
                    break;
                }
            }
            if (low > high)
                pos = high;
        }

        boolean isInsertLatest = this.isLatest && (pos == childRID.size() - 1);
        getStatsBlockNonRoot(manager, childRID.get(pos), this, measurement, degree, dataType, childStartTime.get(pos), isInsertLatest).insert(data, updateIfExist);
    }

    @Override
    public void appendStats(DataPoint data) throws TimeseriesException {
        this.statistics.insert(data);
        if (!isLatest) parent.appendStats(data);
        setAsDirty();
    }

    @Override
    public void appendStats(Statistics stats) throws TimeseriesException {
        this.statistics.merge(stats);
        setAsDirty();
    }

    @Override
    public void updateStats(DataPoint oldDP, DataPoint newDP) throws TimeseriesException {
        if (!this.statistics.update(oldDP, newDP)){
            this.statistics = Statistics.newEmptyStats(dataType);
            int childSize = childRID.size();
            // latest child is not included in statistics
            if (isLatest) childSize--;
            for (int i=0; i<childSize; i++){
                this.statistics.merge(getStatsBlockNonRoot(manager, childRID.get(i), this, measurement, degree, dataType, childStartTime.get(i), false).statistics);
            }
        }
        if (!isLatest)
            parent.updateStats(oldDP, newDP);
        setAsDirty();
    }

    @Override
    public void addChild(StatsBlock child) throws TimeseriesException {
        int pos = -1;
        if (childRID.size() == 0 || childStartTime.get(0) > child.startTime){
            // insert at head
            pos = 0;
        }else if (childStartTime.get(childRID.size()-1) < child.startTime){
            // insert at tail
            pos = childRID.size();
        }else {
            // binary search
            int low = 0, high = childRID.size() - 1;
            while (low <= high) {
                int mid = (low + high) >>> 1;
                long midStartTime = childStartTime.get(mid);
                if (midStartTime < child.startTime)
                    low = mid + 1;
                else if (midStartTime > child.startTime)
                    high = mid - 1;
                else
                    throw new TimeseriesException("cannot insert child with existing startTime");
            }
            pos = low;
        }

        childRID.add(pos, child.document.getIdentity());
        childStartTime.add(pos, child.startTime);

        if (isLatest){
            if (childRID.size() == degree + 1) {
                // create new internal to store last child
                StatsBlockInternal newInternal = (StatsBlockInternal) manager.newArcadeDocument(PREFIX_STATSBLOCK + measurement, document1 -> {
                    return new StatsBlockInternal(manager, document1, measurement, degree, dataType, childStartTime.get(degree), true);
                });

                StatsBlock lastChild = getStatsBlockNonRoot(manager, childRID.get(degree), newInternal, measurement, degree, dataType, childStartTime.get(degree), true);
                childRID.remove(degree);
                childStartTime.remove(degree);

                newInternal.statistics = lastChild.statistics.clone();
                newInternal.childRID.add(lastChild.document.getIdentity());
                newInternal.childStartTime.add(lastChild.startTime);
                newInternal.save();

                // remake statistics of this block
                this.statistics = Statistics.newEmptyStats(dataType);
                for (int i = 0; i < degree; i++)
                    statistics.merge(getStatsBlockNonRoot(manager, childRID.get(i), this, measurement, degree, dataType, childStartTime.get(i), false).statistics);

                // commit this block
                parent.appendStats(this.statistics);
                this.isLatest = false;

                parent.addChild(newInternal);
            }
        }else if (childRID.size() > degree) {
            // split into 2 blocks
            int totalSize = childRID.size();
            int splitedSize = totalSize / 2;

            StatsBlockInternal newInternal = (StatsBlockInternal) manager.newArcadeDocument(PREFIX_STATSBLOCK + measurement, document1 -> {
                return new StatsBlockInternal(manager, document1, measurement, degree, dataType, childStartTime.get(splitedSize), false);
            });

            // remake statistics of latter block
            Statistics laterHalfStatics = Statistics.newEmptyStats(dataType);
            for (int i = splitedSize; i < totalSize; i++)
                laterHalfStatics.merge(getStatsBlockNonRoot(manager, childRID.get(i), newInternal, measurement, degree, dataType, childStartTime.get(i), false).statistics);
            newInternal.statistics = laterHalfStatics;
            newInternal.childRID = new ArrayList<>(this.childRID.subList(splitedSize, totalSize));
            newInternal.childStartTime = new ArrayList<>(this.childStartTime.subList(splitedSize, totalSize));
            newInternal.save();

            // remake statistics of this block
            childRID = new ArrayList<>(childRID.subList(0, splitedSize));
            childStartTime = new ArrayList<>(childStartTime.subList(0, splitedSize));
            statistics = Statistics.newEmptyStats(dataType);
            for (int i=0; i<splitedSize; i++)
                statistics.merge(getStatsBlockNonRoot(manager, childRID.get(i), this, measurement, degree, dataType, childStartTime.get(i), false).statistics);

            parent.addChild(newInternal);
        }
        setAsDirty();
    }

    @Override
    public Statistics aggregativeQuery(long startTime, long endTime) throws TimeseriesException {
        // empty block
        if (childRID.size() == 0)
            return Statistics.newEmptyStats(dataType);

        int lastChildIndex = childRID.size() - 1;
        Statistics resultStats;
        if (isLatest && endTime >= childStartTime.get(lastChildIndex)) {
            // calc latest child's statistics as it is out of current statistics
            resultStats = getStatsBlockNonRoot(manager, childRID.get(lastChildIndex), this, measurement, degree, dataType, childStartTime.get(lastChildIndex), true)
                    .aggregativeQuery(startTime, endTime);
            lastChildIndex--;
        }else{
            resultStats = Statistics.newEmptyStats(dataType);
        }

        // if range out of this block's statistics
        if (startTime > statistics.lastTime || endTime < statistics.firstTime){
            return resultStats;
        }
        // if range covers this block's statistics
        if (startTime <= statistics.firstTime && endTime >= statistics.lastTime){
            resultStats.merge(this.statistics);
            return resultStats;
        }

        // locate first block
        int pos = -1;
        if (childStartTime.get(0) >= startTime){
            // start from head
            pos = 0;
        }else {
            // binary search
            int low = 0, high = childRID.size() - 1;
            while (low <= high) {
                int mid = (low + high) >>> 1;
                long midStartTime = childStartTime.get(mid);
                if (midStartTime > startTime)
                    high = mid - 1;
                else if (midStartTime < startTime)
                    low = mid + 1;
                else {
                    pos = mid;
                    break;
                }
            }
            if (low > high)
                pos = high;
        }

        // merge non-latest block's statistics
        while (pos <= lastChildIndex && childStartTime.get(pos) <= endTime){
            resultStats.merge(getStatsBlockNonRoot(manager, childRID.get(pos), this, measurement, degree, dataType, childStartTime.get(pos), false)
                    .aggregativeQuery(startTime, endTime));
            pos++;
        }
        return resultStats;
    }

    @Override
    public DataPointSet periodQuery(long startTime, long endTime) throws TimeseriesException {
        // locate first block
        int pos = -1;
        if (childStartTime.get(0) >= startTime){
            // start from head
            pos = 0;
        }else {
            // binary search
            int low = 0, high = childRID.size() - 1;
            while (low <= high) {
                int mid = (low + high) >>> 1;
                long midStartTime = childStartTime.get(mid);
                if (midStartTime > startTime)
                    high = mid - 1;
                else if (midStartTime < startTime)
                    low = mid + 1;
                else {
                    pos = mid;
                    break;
                }
            }
            if (low > high)
                pos = high;
        }

        return getStatsBlockNonRoot(manager, childRID.get(pos), this, measurement, degree, dataType, childStartTime.get(pos), isLatest && (pos == childRID.size()-1)).periodQuery(startTime, endTime);
    }
}
