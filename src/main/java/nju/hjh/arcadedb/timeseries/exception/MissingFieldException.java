package nju.hjh.arcadedb.timeseries.exception;

public class MissingFieldException extends MessageParsingException {
    public MissingFieldException(String fieldName) {
        super("field '"+ fieldName +"' not found");
    }
    public MissingFieldException(String fieldName, String location) {
        super("field '"+ fieldName +"' missing or in wrong format at " + location);
    }
}
