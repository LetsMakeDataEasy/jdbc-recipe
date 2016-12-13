package jdbc.recipe;

public class Config {
    private final String srcUrl;
    private final String srcUser;
    private final String srcPassword;
    private final String targetUrl;
    private final String targetUser;
    private final String targetPassword;
    private final String tableFilter;
    private final int batchSize;
    private final int maxCount;

    public Config(
            String srcUrl, String
            srcUser,
            String srcPassword,
            String targetUrl,
            String targetUser,
            String targetPassword,
            String tableFilter,
            int batchSize,
            int maxCount) {
        this.srcUrl = srcUrl;
        this.srcUser = srcUser;
        this.srcPassword = srcPassword;
        this.targetUrl = targetUrl;
        this.targetUser = targetUser;
        this.targetPassword = targetPassword;
        this.tableFilter = tableFilter;
        this.batchSize = batchSize;
        this.maxCount = maxCount;
    }

    public String getSrcUrl() {
        return srcUrl;
    }

    public String getSrcUser() {
        return srcUser;
    }

    public String getSrcPassword() {
        return srcPassword;
    }

    public String getTargetUrl() {
        return targetUrl;
    }

    public String getTargetUser() {
        return targetUser;
    }

    public String getTargetPassword() {
        return targetPassword;
    }

    public String getTableFilter() {
        return tableFilter;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public int getMaxCount() {
        return maxCount;
    }
}
