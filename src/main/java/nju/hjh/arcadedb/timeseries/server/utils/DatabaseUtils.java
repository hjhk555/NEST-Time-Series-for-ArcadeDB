package nju.hjh.arcadedb.timeseries.server.utils;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.schema.Property;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;
import nju.hjh.arcadedb.timeseries.NestEngine;
import nju.hjh.arcadedb.timeseries.exception.DatabaseException;
import nju.hjh.arcadedb.timeseries.exception.TargetNotFoundException;
import nju.hjh.arcadedb.timeseries.exception.TimeseriesException;

import java.util.*;

public class DatabaseUtils {
    public static final String PROP_OBJECT_ID = "oid";
    public static final String DATABASE_DIR = "./databases";
    public static final HashMap<String, NestEngine> databaseInstances = new HashMap<>();

    /**
     * get database instance, if not exist, create one
     *
     * @param dbName name of database
     * @return database instance
     */
    public static NestEngine getOrCreateNestDatabase(String dbName) {
        synchronized (databaseInstances) {
            NestEngine engine = databaseInstances.get(dbName);
            if (engine != null) return engine;

            DatabaseFactory dbf = new DatabaseFactory(DATABASE_DIR + dbName);
            Database db;
            if (dbf.exists()) {
                db = dbf.open();
            } else {
                db = dbf.create();
            }
            engine = new NestEngine(db);

            databaseInstances.put(dbName, engine);
            return engine;
        }
    }

    public static boolean checkDatabaseExists(String dbName) {
        DatabaseFactory dbf = new DatabaseFactory(DATABASE_DIR + dbName);
        return dbf.exists();
    }

    public static NestEngine getNestDatabase(String dbName) throws DatabaseException {
        synchronized (databaseInstances) {
            NestEngine engine = databaseInstances.get(dbName);
            if (engine != null) return engine;

            DatabaseFactory dbf = new DatabaseFactory(DATABASE_DIR + dbName);
            if (dbf.exists()) {
                engine = new NestEngine(dbf.open());
            } else {
                throw new DatabaseException("database '" + dbName + "' not exists");
            }

            databaseInstances.put(dbName, engine);
            return engine;
        }
    }

    public static NestEngine createNestDatabase(String dbName) throws DatabaseException {
        synchronized (databaseInstances) {
            DatabaseFactory dbf = new DatabaseFactory(DATABASE_DIR + dbName);
            if (dbf.exists())
                throw new DatabaseException("database '" + dbName + "' already exists");
            NestEngine engine = new NestEngine(dbf.create());
            databaseInstances.put(dbName, engine);
            return engine;
        }
    }

    public static void dropDatabase(String dbName) throws DatabaseException {
        synchronized (databaseInstances) {
            NestEngine activeEngine = databaseInstances.get(dbName);
            if (activeEngine != null) {
                synchronized (activeEngine) {
                    activeEngine.getDatabase().drop();
                    databaseInstances.remove(dbName);
                }
            } else {
                DatabaseFactory dbf = new DatabaseFactory(DATABASE_DIR + dbName);
                if (!dbf.exists())
                    throw new DatabaseException("database '" + dbName + "' not exists");
                dbf.open().drop();
            }
        }
    }

    public static void closeAllDatabase(){
        synchronized (databaseInstances) {
            for (NestEngine engine : databaseInstances.values()) {
                engine.getDatabase().close();
            }
        }
    }

    /**
     * use between begin and commit
     *
     * @param objectId unique id of object
     * @return vertex asked
     */
    public static Vertex getOrCreateSingleVertex(Database database, String objectType, String objectId) throws TimeseriesException {
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
    public static Vertex getSingleVertex(Database database, String objectType, String objectId) throws TimeseriesException {
        if (!database.getSchema().existsType(objectType))
            throw new TargetNotFoundException("objectType not exist");

        IndexCursor cursor = database.lookupByKey(objectType, PROP_OBJECT_ID, objectId);
        if (cursor.hasNext()) return cursor.next().asVertex();

        throw new TargetNotFoundException("object under given id '"+objectId+"' not found");
    }
}
