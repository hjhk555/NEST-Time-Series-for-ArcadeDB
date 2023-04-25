package com.arcadedb.timeseries;

import com.arcadedb.database.Database;
import com.arcadedb.database.RID;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.MutableEdge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.VertexType;

import java.util.Iterator;

public class TimeseriesEngine {
    public static final String PREFIX_MEASUREMENT_EDGE = "_meas_";
    public static final int MEASUREMENT_EDGE_BUCKETS = 1;
    public static final int STATSBLOCK_BUCKETS = 1;
    public final Database database;
    public final ArcadeDocumentManager manager;

    public TimeseriesEngine(Database database) {
        this.database = database;
        this.manager = new ArcadeDocumentManager(database);
    }

    public StatsBlockRoot getStatsTreeRoot(Vertex object, String measurement) throws TimeseriesException {
        final String measurementEdgeType = PREFIX_MEASUREMENT_EDGE+measurement;
        final String objectMeasurement = object.getIdentity().getBucketId()+"_"+measurement;

        // measurement edge not exist
        if (!database.getSchema().existsType(measurementEdgeType))
            throw new TimeseriesException("measurement edgeType "+measurementEdgeType+" not exist");

        // object's statsBlock document not exist
        if (!database.getSchema().existsType(StatsBlock.PREFIX_STATSBLOCK+objectMeasurement))
            throw new TimeseriesException("object's statsBlock vertexType "+StatsBlock.PREFIX_STATSBLOCK+objectMeasurement+" not exist");

        Iterator<Edge> edgeIterator = object.getEdges(Vertex.DIRECTION.OUT, measurementEdgeType).iterator();

        // no existing statsBlockRoot
        if (!edgeIterator.hasNext()){
            throw new TimeseriesException("object has no measurement "+measurement);
        }else{
            Edge measurementedge = edgeIterator.next();
            // have more than 1 statsTree
            if (edgeIterator.hasNext()){
                throw new TimeseriesException("object has more than 1 outEdge to measurement "+measurement);
            }
            return StatsBlock.getStatsBlockRoot(manager, measurementedge.getIn(), objectMeasurement);
        }
    }

    public StatsBlockRoot getOrNewStatsTreeRoot(Vertex object, String measurement, DataType dataType, int statsTreeDegree) throws TimeseriesException {
        final String measurementEdgeType = PREFIX_MEASUREMENT_EDGE+measurement;
        final String objectMeasurement = object.getIdentity().getBucketId()+"_"+measurement;

        // create measurement edge if not exist
        if (!database.getSchema().existsType(measurementEdgeType))
            database.getSchema().createEdgeType(measurementEdgeType, MEASUREMENT_EDGE_BUCKETS);

        // create object's measurement document if not exist
        if (!database.getSchema().existsType(StatsBlock.PREFIX_STATSBLOCK+objectMeasurement)) {
            DocumentType statsBlockType = database.getSchema().createDocumentType(StatsBlock.PREFIX_STATSBLOCK + objectMeasurement, STATSBLOCK_BUCKETS);
            statsBlockType.createProperty("stat", "BINARY");
            statsBlockType.createProperty("data", "BINARY");
        }

        Iterator<Edge> edgeIterator = object.getEdges(Vertex.DIRECTION.OUT, measurementEdgeType).iterator();

        // get root of statsTree
        StatsBlockRoot treeRoot;
        // no existing statsBlockRoot, create one
        if (!edgeIterator.hasNext()){
            treeRoot = StatsBlock.newStatsTree(manager, objectMeasurement, dataType, statsTreeDegree);
            // link edge
            object.newEdge(measurementEdgeType, treeRoot.document, false).save();
        }else{
            Edge measurementedge = edgeIterator.next();
            // have more than 1 statsTree
            if (edgeIterator.hasNext()){
                throw new TimeseriesException("object has out edge to more than 1 target measurement");
            }
            treeRoot = StatsBlock.getStatsBlockRoot(manager, measurementedge.getIn(), objectMeasurement);
        }

        return treeRoot;
    }

    public boolean insertDataPoint(Vertex object, String measurement, DataType dataType, DataPoint dataPoint) throws TimeseriesException {
        return insertDataPoint(object, measurement, dataType, dataPoint, StatsBlock.DEFAULT_TREE_DEGREE);
    }

    public boolean insertDataPoint(Vertex object, String measurement, DataType dataType, DataPoint dataPoint, int statsTreeDegree) throws TimeseriesException {
        StatsBlockRoot root = getOrNewStatsTreeRoot(object, measurement, dataType, statsTreeDegree);
        dataPoint = root.dataType.checkAndConvertDataPoint(dataPoint);
        return root.insert(dataPoint);
    }

    public Statistics aggregativeQuery(Vertex object, String measurement, long startTime, long endTime) throws TimeseriesException {
        return getStatsTreeRoot(object, measurement).aggregativeQuery(startTime, endTime);
    }

    public DataPointSet periodQuery(Vertex object, String measurement, long startTime, long endTime) throws TimeseriesException {
        return getStatsTreeRoot(object, measurement).periodQuery(startTime, endTime);
    }

    public void begin(){
        database.begin();
    }

    public void commit(){
        manager.saveAll();
        database.commit();
    }

    public void rollback(){
        manager.clearCache();
        database.rollback();
    }

}
