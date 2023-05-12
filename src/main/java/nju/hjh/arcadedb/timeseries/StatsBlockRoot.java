package nju.hjh.arcadedb.timeseries;

import com.arcadedb.database.Binary;
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import nju.hjh.arcadedb.timeseries.datapoint.DataPoint;
import nju.hjh.arcadedb.timeseries.exception.TimeseriesException;
import nju.hjh.arcadedb.timeseries.statistics.Statistics;

import java.util.ArrayList;

public class StatsBlockRoot extends StatsBlock{
    public static final byte BLOCK_TYPE = 0;

    /**
     * size of stat header without statistics and child list:
     * block type(1B) + degree(4B) + data type(5B) + child size(4B)
     */
    public static final int HEADER_WITHOUT_STATS_AND_CHILD = 14;

    public ArrayList<RID> childRID = new ArrayList<>();
    public ArrayList<Long> childStartTime = new ArrayList<>();

    public StatsBlockRoot(ArcadeDocumentManager manager, Document document, String metric, int degree, DataType dataType) {
        // root is always latest and start at 0
        super(manager, document, metric, degree, dataType, 0, true);
    }

    @Override
    public void setParent(StatsBlock parent) {
        // ignore
    }

    @Override
    public MutableDocument serializeDocument() throws TimeseriesException {
        int statSize = HEADER_WITHOUT_STATS_AND_CHILD + Statistics.maxBytesRequired(dataType) + degree * CHILD_SIZE;

        MutableDocument modifiedDocument = document.modify();
        Binary binary = new Binary(statSize, false);
        binary.putByte(BLOCK_TYPE);
        binary.putInt(degree);
        dataType.serialize(binary);
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
        modifiedDocument.set("stat", binary.toByteArray());

        return modifiedDocument;
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

        boolean isInsertLatest = pos == childRID.size() - 1;
        getStatsBlockNonRoot(manager, childRID.get(pos), this, metric, degree, dataType, childStartTime.get(pos), isInsertLatest).insert(data, updateIfExist);
    }

    @Override
    public void appendStats(DataPoint data) throws TimeseriesException {
        this.statistics.insert(data);
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
            // root's last child is always latest
            this.statistics = Statistics.newEmptyStats(dataType);
            int childSize = childRID.size()-1;
            for (int i=0; i<childSize; i++){
                this.statistics.merge(getStatsBlockNonRoot(manager, childRID.get(i), this, metric, degree, dataType, childStartTime.get(i), false).statistics);
            }
        }
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

        if (childRID.size() == degree){
            StatsBlockInternal newInternal = (StatsBlockInternal) manager.newArcadeDocument(PREFIX_STATSBLOCK+ metric, document1 -> {
                return new StatsBlockInternal(manager, document1, metric, degree, dataType, 0, true);
            });
            newInternal.childRID = this.childRID;
            newInternal.childStartTime = this.childStartTime;
            newInternal.statistics = this.statistics;
            // latest child's statistics not included
            this.statistics = Statistics.newEmptyStats(dataType);
            newInternal.save();

            this.childRID = new ArrayList<>();
            this.childStartTime = new ArrayList<>();
            this.childRID.add(newInternal.document.getIdentity());
            this.childStartTime.add(0L);
        }
        setAsDirty();
    }

    @Override
    public Statistics aggregativeQuery(long startTime, long endTime) throws TimeseriesException {
        // empty block
        if (childRID.size() == 0)
            throw new TimeseriesException("root has no child to calc statistics");


        int lastChildIndex = childRID.size() - 1;
        Statistics resultStats;
        if (endTime >= childStartTime.get(lastChildIndex)) {
            // calc latest child's statistics as it is out of current statistics
            resultStats = getStatsBlockNonRoot(manager, childRID.get(lastChildIndex), this, metric, degree, dataType, childStartTime.get(lastChildIndex), true)
                    .aggregativeQuery(startTime, endTime);
            lastChildIndex--;
        }else{
            resultStats = Statistics.newEmptyStats(dataType);
        }

        // if range out of root's statistics
        if (startTime > statistics.lastTime || endTime < statistics.firstTime){
            return resultStats;
        }
        // if range covers root's statistics
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
            resultStats.merge(getStatsBlockNonRoot(manager, childRID.get(pos), this, metric, degree, dataType, childStartTime.get(pos), false).aggregativeQuery(startTime, endTime));
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

        return getStatsBlockNonRoot(manager, childRID.get(pos), this, metric, degree, dataType, childStartTime.get(pos), pos == childRID.size()-1).periodQuery(startTime, endTime);
    }
}
