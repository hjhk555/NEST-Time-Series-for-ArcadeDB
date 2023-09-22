package nju.hjh.utils.log;

public class LogTag {
    protected final String tagId;
    final LogTagContent content;
    public final boolean showOnScreen;

    public LogTag(String tagId, LogTagContent content, boolean showOnScreen) {
        this.tagId = tagId;
        this.content = content;
        this.showOnScreen = showOnScreen;
    }

    public String getContent(){
        return content.getContent();
    }
}
