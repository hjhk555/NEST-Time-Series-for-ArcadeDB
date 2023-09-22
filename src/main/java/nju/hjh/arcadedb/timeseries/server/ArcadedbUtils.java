package nju.hjh.arcadedb.timeseries.server;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Property;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;
import nju.hjh.arcadedb.timeseries.exception.DatabaseManageException;
import nju.hjh.arcadedb.timeseries.exception.SQLParsingException;
import nju.hjh.arcadedb.timeseries.exception.TargetNotFoundException;
import nju.hjh.arcadedb.timeseries.exception.TimeseriesException;

import java.util.*;

public class ArcadedbUtils {
    public static final HashMap<String, Database> databaseInstances = new HashMap<>();

    /**
     * get database instance
     * @param dbName name of database
     * @return database instance
     */
    public static Database getOrCreateDatabase(String dbName){
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

    public static Database createDatabase(String dbName) throws DatabaseManageException {
        synchronized (databaseInstances) {
            DatabaseFactory dbf = new DatabaseFactory(ServerUtils.DATABASE_DIR + dbName);
            if (dbf.exists())
                throw new DatabaseManageException("database " + dbName + " already exists");
            Database newDatabase = dbf.create();
            databaseInstances.put(dbName, newDatabase);
            return newDatabase;
        }
    }

    public static void dropDatabase(String dbName) throws DatabaseManageException {
        synchronized (databaseInstances){
            Database activeDatabase = databaseInstances.get(dbName);
            if (activeDatabase != null){
                synchronized (activeDatabase){
                    activeDatabase.drop();
                    databaseInstances.remove(dbName);
                }
            }else{
                DatabaseFactory dbf = new DatabaseFactory(ServerUtils.DATABASE_DIR + dbName);
                if (!dbf.exists())
                    throw new DatabaseManageException("database "+ dbName + " not exists");
                dbf.open().drop();
            }
        }
    }

    /**
     * use between begin and commit
     * @param tags tags appended to objectType
     * @return vertex asked
     */
    public static Vertex getOrCreateSingleVertex(Database database, String objectType, Map<String, String> tags) throws TimeseriesException {
        // check object type
        boolean isNewVertex = !database.getSchema().existsType(objectType);
        VertexType metricVertex = database.getSchema().getOrCreateVertexType(objectType);

        // check tags as properties
        for (String tagKey : tags.keySet()) {
            if (!metricVertex.existsProperty(tagKey)) {
                isNewVertex = true;
                Property newKey = metricVertex.createProperty(tagKey, Type.STRING);
                newKey.createIndex(Schema.INDEX_TYPE.LSM_TREE, false);
            }
        }

        // find existing vertex
        if (!isNewVertex) {
            String sql = getQuerySql(objectType, tags);
            ResultSet rs;
            try {
                rs = database.query("SQL", sql);
            } catch (CommandSQLParsingException e) {
                throw new SQLParsingException("error parsing SQL " + sql);
            }
            while (rs.hasNext()) {
                Optional<Vertex> optVertex = rs.next().getVertex();
                if (optVertex.isEmpty()) continue;
                Vertex vertex = optVertex.get();
                if (vertex.toMap(false).size() == tags.size()) return vertex;
            }
        }

        // create new vertex
        MutableVertex newVertex = database.newVertex(objectType);
        for (Map.Entry<String, String> entry : tags.entrySet()){
            newVertex.set(entry.getKey(), entry.getValue());
        }
        newVertex.save();
        return newVertex;
    }

    /**
     * use between begin and commit
     * @param tags tags appended to objectType
     * @return vertex asked
     */
    public static Vertex getSingleVertex(Database database, String objectType, Map<String, String> tags) throws TimeseriesException {
        if (!database.getSchema().existsType(objectType))
            throw new TargetNotFoundException("objectType not exist");

        String sql = getQuerySql(objectType, tags);
        ResultSet rs;
        try {
            rs = database.query("SQL", sql);
        } catch (CommandSQLParsingException e) {
            throw new SQLParsingException("error parsing SQL " + sql);
        }
        while (rs.hasNext()){
            Optional<Vertex> optVertex = rs.next().getVertex();
            if (optVertex.isEmpty()) continue;
            Vertex vertex = optVertex.get();
            if (vertex.toMap(false).size() == tags.size()) return vertex;
        }

        throw new TargetNotFoundException("object vertex under given tags not found");
    }


    public static ResultSet getVertices(Database database, String objectType, Map<String, String> tags) throws TimeseriesException {
        if (!database.getSchema().existsType(objectType))
            throw new TargetNotFoundException("objectType not exist");

        ArrayList<Vertex> vertices = new ArrayList<>();
        String sql = getQuerySql(objectType, tags);
        ResultSet rs;
        try {
            rs = database.query("SQL", sql);
        } catch (CommandSQLParsingException e) {
            throw new SQLParsingException("error parsing SQL " + sql);
        }
        return rs;
    }

    private static String getQuerySql(String metric, Map<String, String> tags){
        StringBuilder sqlBuilder = new StringBuilder(String.format("SELECT FROM %s", metric));
        if (!tags.isEmpty()) {
            boolean firstTag = true;
            for (Map.Entry<String, String> entry : tags.entrySet()) {
                sqlBuilder.append(firstTag ? " WHERE " : " AND ");
                firstTag = false;
                sqlBuilder.append(String.format("%s='%s'", entry.getKey(), entry.getValue()));
            }
        }
        return sqlBuilder.toString();
    }
}
