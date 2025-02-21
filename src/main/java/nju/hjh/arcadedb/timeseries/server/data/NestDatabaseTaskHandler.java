package nju.hjh.arcadedb.timeseries.server.data;

import com.arcadedb.database.Database;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.schema.Property;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;
import nju.hjh.arcadedb.timeseries.NestEngine;
import nju.hjh.arcadedb.timeseries.exception.TargetNotFoundException;
import nju.hjh.arcadedb.timeseries.exception.TimeseriesException;
import nju.hjh.arcadedb.timeseries.server.bo.Metric;
import nju.hjh.arcadedb.timeseries.server.bo.TimeseriesInsertTask;
import nju.hjh.arcadedb.timeseries.server.utils.ResponseUtils;

import java.util.HashMap;
import java.util.Map;

public class NestDatabaseTaskHandler {
    public static final String PROP_OBJECT_ID = "oid";
    /**
     * use between begin and commit
     *
     * @param objectId unique id of object
     * @return vertex asked
     */
    private static Vertex getOrCreateSingleVertex(Database database, String objectType, String objectId) {
        // check object type
        boolean isNewVertex = !database.getSchema().existsType(objectType);
        VertexType metricVertex = database.getSchema().getOrCreateVertexType(objectType);

        // check object id
        if (!metricVertex.existsProperty(PROP_OBJECT_ID)){
            Property propObjectId = metricVertex.createProperty(PROP_OBJECT_ID, Type.STRING);
            propObjectId.createIndex(Schema.INDEX_TYPE.LSM_TREE, true);
            isNewVertex = true;
        }

        // find existing vertex
        if (!isNewVertex) {
            IndexCursor cursor = database.lookupByKey(objectType, PROP_OBJECT_ID, objectId);
            if (cursor.hasNext()) return cursor.next().asVertex();
        }

        // create new vertex
        MutableVertex newVertex = database.newVertex(objectType);
        newVertex.set(PROP_OBJECT_ID, objectId);
        newVertex.save();
        return newVertex;
    }

    /**
     * use between begin and commit
     *
     * @param objectId unique id of object
     * @return vertex asked
     */
    private static Vertex getSingleVertex(Database database, String objectType, String objectId) throws TimeseriesException {
        if (!database.getSchema().existsType(objectType))
            // no object exists in database
            throw new TargetNotFoundException("object under given id '"+objectId+"' not found");

        IndexCursor cursor = database.lookupByKey(objectType, PROP_OBJECT_ID, objectId);
        if (cursor.hasNext()) return cursor.next().asVertex();

        throw new TargetNotFoundException("object under given id '"+objectId+"' not found");
    }

    public static Map<String, Object> handleTimeseriesInsertTask(NestEngine engine, TimeseriesInsertTask task){
        engine.begin();
        try{
            for (Metric metric : task.getMetrics()) {
                MutableVertex vtxObject = getOrCreateSingleVertex(engine.getDatabase(), metric.getObjectType(), metric.getObjectId()).modify();
                for (Map.Entry<Long, Object> datapoint: metric.getDataPoints().entrySet()){
                    engine.insertDataPoint(vtxObject, metric.getMetricName(), datapoint.getKey(), datapoint.getValue(), metric.getStrategy());
                }
            }
            engine.commit();
            Map<String, Object> result = new HashMap<>();
            result.put("status", "ok");
            return result;
        } catch (Exception e){
            engine.rollback();
            return ResponseUtils.getExceptionResponse(e);
        }
    }
}
