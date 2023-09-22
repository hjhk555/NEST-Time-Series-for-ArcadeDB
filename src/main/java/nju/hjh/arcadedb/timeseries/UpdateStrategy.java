package nju.hjh.arcadedb.timeseries;

public class UpdateStrategy {
    public static UpdateStrategy IGNORE;
    public static UpdateStrategy ERROR;
    public static UpdateStrategy UPDATE;
    public static UpdateStrategy APPEND;

    static {
        IGNORE = new UpdateStrategy(TSBaseUpdateStrategy.IGNORE);
        ERROR = new UpdateStrategy(TSBaseUpdateStrategy.ERROR);
        UPDATE = new UpdateStrategy(TSBaseUpdateStrategy.UPDATE);
        APPEND = new UpdateStrategy(TSBaseUpdateStrategy.APPEND);
    }

    public enum TSBaseUpdateStrategy {
        IGNORE,
        ERROR,
        UPDATE,
        APPEND
    }

    public TSBaseUpdateStrategy baseStrategy;
    public String separator;

    public UpdateStrategy(TSBaseUpdateStrategy baseStrategy){
        this(baseStrategy, "+");
    }

    public UpdateStrategy(TSBaseUpdateStrategy baseStrategy, String separator) {
        this.baseStrategy = baseStrategy;
        this.separator = separator;
    }
}
