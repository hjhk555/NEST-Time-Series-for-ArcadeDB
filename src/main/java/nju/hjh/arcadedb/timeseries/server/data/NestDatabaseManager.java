package nju.hjh.arcadedb.timeseries.server.data;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import lombok.Setter;
import nju.hjh.arcadedb.timeseries.NestEngine;
import nju.hjh.arcadedb.timeseries.exception.DatabaseException;
import nju.hjh.arcadedb.timeseries.exception.MessageParsingException;
import nju.hjh.arcadedb.timeseries.server.bo.DatabaseTask;
import nju.hjh.arcadedb.timeseries.server.bo.TimeseriesInsertTask;
import nju.hjh.arcadedb.timeseries.server.utils.ResponseUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class NestDatabaseManager {
    public static final String DATABASE_DIR = "./databases";
    private static final HashMap<String, NestDatabaseManager> DATABASE_INSTANCES = new HashMap<>();

    private static class TaskWithFuture{
        public DatabaseTask task;
        public CompletableFuture<Map<String, Object>> resultFuture;

        public TaskWithFuture(DatabaseTask task, CompletableFuture<Map<String, Object>> resultFuture) {
            this.task = task;
            this.resultFuture = resultFuture;
        }
    }

    private interface DatabaseFetcher{
        public Database fetch();
    }

    private interface DatabaseDestoryer{
        public void destroy(Database database);
    }

    private static class NestDatabaseWorker implements Runnable{
        private static final Map<String, Object> INTERRUPT_RESPONSE = ResponseUtils.getExceptionResponse(new DatabaseException("worker interrupted, server shutting down or database dropping"));
        private final BlockingQueue<TaskWithFuture> taskQueue = new LinkedBlockingQueue<>();
        private final DatabaseFetcher fetcher;
        @Setter
        private DatabaseDestoryer destoryer = Database::close;
        private final CompletableFuture<Void> databaseStartup = new CompletableFuture<>();

        public NestDatabaseWorker(DatabaseFetcher fetcher) {
            this.fetcher = fetcher;
        }

        public void submit(TaskWithFuture task) throws InterruptedException {
            taskQueue.put(task);
        }

        public void waitDatabaseStartup() throws DatabaseException {
            try{
                databaseStartup.get();
            }catch (ExecutionException e){
                throw new DatabaseException("unexpected error when starting database");
            }catch (InterruptedException e){
                throw new DatabaseException("interrupted when starting database");
            }
        }

        @Override
        public void run() {
            NestEngine engine = new NestEngine(fetcher.fetch());
            databaseStartup.complete(null);
            try {
                while (true) {
                    TaskWithFuture task = taskQueue.poll(10, TimeUnit.SECONDS);
                    if (task == null) continue;
                    DatabaseTask innerTask = task.task;
                    CompletableFuture<Map<String, Object>> resultFuture = task.resultFuture;
                    if (innerTask instanceof TimeseriesInsertTask insertTask){
                        resultFuture.complete(NestDatabaseTaskHandler.handleTimeseriesInsertTask(engine, insertTask));
                    }else {
                        resultFuture.complete(ResponseUtils.getExceptionResponse(new MessageParsingException("unknown task class "+innerTask.getClass().getName())));
                    }
                }
            } catch (InterruptedException e){
                TaskWithFuture task;
                while ((task = taskQueue.poll()) != null){
                    task.resultFuture.complete(INTERRUPT_RESPONSE);
                }
                destoryer.destroy(engine.getDatabase());
            }
        }
    }

    private final Logger logger = LoggerFactory.getLogger(NestDatabaseManager.class);
    private final String dbName;
    private final DatabaseFactory dbFactory;
    private NestDatabaseWorker worker;
    private Thread workerThread;
    private AtomicBoolean isAvailable = new AtomicBoolean(true);

    private NestDatabaseManager(String name){
        this.dbName = name;
        dbFactory = new DatabaseFactory(DATABASE_DIR + name);
    }

    /**
     * get database manager, if not exist, create one
     *
     * @param dbName name of database
     * @return database manager
     */
    public static NestDatabaseManager getDatabaseManager(String dbName) {
        synchronized (DATABASE_INSTANCES) {
            NestDatabaseManager manager = DATABASE_INSTANCES.get(dbName);
            if (manager != null) return manager;
            manager = new NestDatabaseManager(dbName);
            DATABASE_INSTANCES.put(dbName, manager);
            return manager;
        }
    }

    public static void removeDatabaseManager(String dbName) {
        synchronized (DATABASE_INSTANCES) {
            DATABASE_INSTANCES.remove(dbName);
        }
    }

    public static void closeAllDatabase(){
        synchronized (DATABASE_INSTANCES) {
            List<NestDatabaseManager> managers = DATABASE_INSTANCES.values().stream().toList();
            for (NestDatabaseManager manager : managers) {
                manager.close();
            }
        }
    }

    public boolean isDatabaseExists() {
        return dbFactory.exists();
    }

    public void createDatabase() throws DatabaseException{
        if (!isAvailable.get()) throw new DatabaseException("database manager of '" + dbName + "' already closed");
        if (isDatabaseExists()) {
            throw new DatabaseException("database '" + dbName + "' already exists");
        }
        if (worker != null) {
            throw new DatabaseException("a worker of '" + dbName + "' already running");
        }
        // start worker and create database
        worker = new NestDatabaseWorker(dbFactory::create);
        workerThread = new Thread(worker);
        workerThread.start();
        worker.waitDatabaseStartup();
    }

    private void open() throws DatabaseException{
        if (!isDatabaseExists()) throw new DatabaseException("database '" + dbName + "' not exists");
        if (worker != null) {
            throw new DatabaseException("a worker of '" + dbName + "' already running");
        }
        // start worker and open database
        worker = new NestDatabaseWorker(dbFactory::open);
        workerThread = new Thread(worker);
        workerThread.start();
        worker.waitDatabaseStartup();
    }

    private void stopWorker() throws InterruptedException {
        if (workerThread != null) {
            workerThread.interrupt();
            workerThread.join();
        }
    }

    private void close(){
        isAvailable.set(false);
        try {
            stopWorker();
        } catch (InterruptedException e){
            logger.warn("Interrupted while closing NestDatabaseManager on {}", dbName);
        } finally {
            removeDatabaseManager(dbName);
        }
    }

    public void drop() throws DatabaseException{
        if (!isAvailable.get()) throw new DatabaseException("database manager of '" + dbName + "' already closed");
        isAvailable.set(false);
        try {
            if (worker == null) open();
            worker.setDestoryer(Database::drop);
            stopWorker();
        } catch (InterruptedException e){
            logger.warn("Interrupted while dropping database {}", dbName);
        } finally {
            removeDatabaseManager(dbName);
        }
    }

    public CompletableFuture<Map<String, Object>> submitTask(DatabaseTask task) throws DatabaseException{
        if (!isAvailable.get()) throw new DatabaseException("database manager of '" + dbName + "' already closed");
        CompletableFuture<Map<String, Object>> resultFuture = new CompletableFuture<>();
        try {
            if (worker == null) open();
            worker.submit(new TaskWithFuture(task, resultFuture));
        } catch (InterruptedException e){
            logger.warn("Interrupted while closing NestDatabaseManager on '{}'", dbName);
            throw new DatabaseException("Interrupted while submitting task to '"+dbName+"'");
        }
        return resultFuture;
    }
}
