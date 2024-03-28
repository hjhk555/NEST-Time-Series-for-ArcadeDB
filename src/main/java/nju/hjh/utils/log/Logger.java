package nju.hjh.utils.log;

import nju.hjh.utils.exception.ExceptionSerializer;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;

public class Logger {
    static SimpleDateFormat fileNameTimeFormat = new SimpleDateFormat("yy_MM_dd HH_mm_ss");
    static String lineSeperator = "\n";
    static HashMap<String, Logger> pureLoggerInstances = new HashMap<>();

    final FileWriter logWriter;
    ArrayList<LogTag> tagList = new ArrayList<>();
    boolean autoFlush = true;

    private static String getSpaceString(int len){
        StringBuilder builder = new StringBuilder();
        for (int i=0; i<len; i++)
            builder.append(" ");
        return builder.toString();
    }

    public static Logger getPureLogger(){
        return getPureLogger("");
    }

    // logger with timestamp tag only
    public static Logger getPureLogger(String fileId){
        Logger pureLogger = pureLoggerInstances.get(fileId);
        if (pureLogger != null)
            return pureLogger.clone();

        String filePath = "./hjhkLog/" + (fileId.length()>0 ? fileId + "/" : "") + fileNameTimeFormat.format(System.currentTimeMillis()) + ".txt";
        File newFile = new File(filePath);
        try {
            if (!newFile.getParentFile().exists()) {
                newFile.getParentFile().mkdirs();
            }
            if (!newFile.exists()) {
                newFile.createNewFile();
            }
            FileWriter writer = new FileWriter(newFile);
            pureLogger = new Logger(writer);
        } catch (IOException e) {
            System.err.println("failed to create writer for fileId "+fileId+", any log committed to this fileId will not be recorded.");
            e.printStackTrace();
            pureLogger = new Logger(null);
        }

        pureLogger.appendTag("timestamp", TimestampContent.getInstance(), false);
        pureLoggerInstances.put(fileId, pureLogger);
        return pureLogger.clone();
    }

    public Logger clone(){
        Logger newLogger = new Logger(logWriter);
        newLogger.tagList.addAll(this.tagList);
        return newLogger;
    }

    public Logger(FileWriter writer){
        this.logWriter = writer;
    }

    public boolean appendTag(String tagId, LogTagContent tagContent){
        return appendTag(tagId, tagContent, true);
    }

    public boolean appendTag(String tagId, LogTagContent tagContent, boolean showOnScreen){
        if (getTagIndex(tagId) >=0 ) return false;
        tagList.add(new LogTag(tagId, tagContent, showOnScreen));
        return true;
    }

    public int getTagIndex(String tagId){
        for (int index = 0; index < tagList.size(); index++){
            if (tagList.get(index).tagId.equals(tagId))
                return index;
        }
        return -1;
    }

    public void enableAutoFlush(){
        autoFlush = true;
    }

    public void disableAutoFlush(){
        autoFlush = false;
    }

    private void logRawContent(String logConent) {
        if (logWriter == null)
            return;
        synchronized (logWriter) {
            try {
                logWriter.write(logConent + lineSeperator);
                if (autoFlush) logWriter.flush();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private String getTaggedContentForScreen(String logContent){
        StringBuilder strTags = new StringBuilder();
        for (LogTag tag : tagList){
            if (tag.showOnScreen)
                strTags.append("[").append(tag.getContent()).append("] ");
        }
        logContent = strTags + logContent;
        return logContent.replaceAll(lineSeperator, lineSeperator + getSpaceString(strTags.length()));
    }

    private String getTaggedContentForLog(String logContent){
        StringBuilder strTags = new StringBuilder();
        for (LogTag tag : tagList){
            strTags.append("[").append(tag.getContent()).append("] ");
        }
        logContent = strTags + logContent;
        return logContent.replaceAll(lineSeperator, lineSeperator + getSpaceString(strTags.length()));
    }

    public void logSilent(String str, Object... args){
        logRawContent(getTaggedContentForLog(String.format(str, args)));
    }

    public void logOnStdout(String str, Object... args){
        String logContent = String.format(str, args);
        logRawContent(getTaggedContentForLog(logContent));
        System.out.println(getTaggedContentForScreen(logContent));
    }

    public void logOnStderr(String str, Object... args){
        String logContent = String.format(str, args);
        logRawContent(getTaggedContentForLog(logContent));
        System.err.println(getTaggedContentForScreen(logContent));
    }

    public void logException(Exception e){
        logOnStderr(ExceptionSerializer.serializeAll(e));
    }

    public void removeTag(String tagId){
        int index = getTagIndex(tagId);
        if (index < 0)
            return;
        tagList.remove(index);
    }

    public void flush(){
        if (logWriter == null)
            return;
        synchronized (logWriter) {
            try {
                logWriter.flush();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
