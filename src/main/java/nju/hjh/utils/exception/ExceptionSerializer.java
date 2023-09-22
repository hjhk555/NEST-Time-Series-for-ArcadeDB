package nju.hjh.utils.exception;

public class ExceptionSerializer {
    public static String serializeAll(Exception e){
        return e.getClass().getName()+" : "+e.getMessage()+"\n"+serializeStackTrace(e);
    }

    public static String serializeStackTrace(Exception e){
        return serializeStackTrace(e, "    at ");
    }

    public static String serializeStackTrace(Exception e, String prefix){
        StackTraceElement[] stackTraceList = e.getStackTrace();
        int len = stackTraceList.length;
        if (len == 0) return "";

        StringBuilder strBuilder = new StringBuilder();
        strBuilder.append(prefix).append(stackTraceList[0]);
        for (int i=1; i<len; i++){
            strBuilder.append("\n").append(prefix).append(stackTraceList[i]);
        }

        return strBuilder.toString();
    }
}
