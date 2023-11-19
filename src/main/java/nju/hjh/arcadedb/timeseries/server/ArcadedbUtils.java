package nju.hjh.arcadedb.timeseries.server;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Property;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;
import nju.hjh.arcadedb.timeseries.exception.DatabaseException;
import nju.hjh.arcadedb.timeseries.exception.SQLParsingException;
import nju.hjh.arcadedb.timeseries.exception.TargetNotFoundException;
import nju.hjh.arcadedb.timeseries.exception.TimeseriesException;

import java.util.*;

public class ArcadedbUtils {
    public static final String PROP_OBJECT_ID = "oid";
    public static final HashMap<String, Database> databaseInstances = new HashMap<>();

    /**
     * get database instance, if not exist, create one
     *
     * @param dbName name of database
     * @return database instance
     */
    public static Database getOrCreateDatabase(String dbName) {
        synchronized (databaseInstances) {
            Database database = databaseInstances.get(dbName);
            if (database != null) return database;

            DatabaseFactory dbf = new DatabaseFactory(ServerUtils.DATABASE_DIR + dbName);
            if (dbf.exists()) {
                database = dbf.open();
            } else {
                database = dbf.create();
            }

            databaseInstances.put(dbName, database);
            return database;
        }
    }

    public static Database getDatabase(String dbName) throws DatabaseException {
        synchronized (databaseInstances) {
            Database database = databaseInstances.get(dbName);
            if (database != null) return database;

            DatabaseFactory dbf = new DatabaseFactory(ServerUtils.DATABASE_DIR + dbName);
            if (dbf.exists()) {
                database = dbf.open();
            } else {
                throw new DatabaseException("database '" + dbName + "' not exists");
            }

            databaseInstances.put(dbName, database);
            return database;
        }
    }

    public static Database createDatabase(String dbName) throws DatabaseException {
        synchronized (databaseInstances) {
            DatabaseFactory dbf = new DatabaseFactory(ServerUtils.DATABASE_DIR + dbName);
            if (dbf.exists())
                throw new DatabaseException("database '" + dbName + "' already exists");
            Database newDatabase = dbf.create();
            databaseInstances.put(dbName, newDatabase);
            return newDatabase;
        }
    }

    public static void dropDatabase(String dbName) throws DatabaseException {
        synchronized (databaseInstances) {
            Database activeDatabase = databaseInstances.get(dbName);
            if (activeDatabase != null) {
                synchronized (activeDatabase) {
                    activeDatabase.drop();
                    databaseInstances.remove(dbName);
                }
            } else {
                DatabaseFactory dbf = new DatabaseFactory(ServerUtils.DATABASE_DIR + dbName);
                if (!dbf.exists())
                    throw new DatabaseException("database '" + dbName + "' not exists");
                dbf.open().drop();
            }
        }
    }

    /**
     * use between begin and commit
     *
     * @param objectId unique id of object
     * @param tags tags to write in new object
     * @return vertex asked
     */
    public static Vertex getOrCreateSingleVertex(Database database, String objectType, String objectId, Map<String, String> tags) throws TimeseriesException {
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
        for (Map.Entry<String, String> tag : tags.entrySet()){
            newVertex.set(tag.getKey(), tag.getValue());
        }
        newVertex.save();
        return newVertex;
    }

    /**
     * use between begin and commit
     *
     * @param objectId unique id of object
     * @return vertex asked
     */
    public static Vertex getSingleVertex(Database database, String objectType, String objectId) throws TimeseriesException {
        if (!database.getSchema().existsType(objectType))
            throw new TargetNotFoundException("objectType not exist");

        IndexCursor cursor = database.lookupByKey(objectType, PROP_OBJECT_ID, objectId);
        if (cursor.hasNext()) return cursor.next().asVertex();

        throw new TargetNotFoundException("object under given id '"+objectId+"' not found");
    }
}
