package jdbc.recipe;

import javaslang.control.Option;
import javaslang.control.Try;

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
    private final String transactionFile;

    public Config(
            String srcUrl, String
            srcUser,
            String srcPassword,
            String targetUrl,
            String targetUser,
            String targetPassword,
            String tableFilter,
            int batchSize,
            int maxCount, String transactionFile) {
        this.srcUrl = srcUrl;
        this.srcUser = srcUser;
        this.srcPassword = srcPassword;
        this.targetUrl = targetUrl;
        this.targetUser = targetUser;
        this.targetPassword = targetPassword;
        this.tableFilter = tableFilter;
        this.batchSize = batchSize;
        this.maxCount = maxCount;
        this.transactionFile = transactionFile;
    }

    static String env(String source_url) {
        return System.getenv(source_url);
    }

    static int intEnv(String key, int defaultValue) {
        return Try.of(() -> Integer.valueOf(env(key))).getOrElse(defaultValue);
    }

    static Config fromEnv() {
        String sUrl = env("source.url");
        String sUser = env("source.user");
        String sPassword = env("source.password");
        String tUrl = env("target.url");
        String tUser = env("target.user");
        String tPassword = env("target.password");
        String tableFilter = env("table.filter");
        int batchSize = intEnv("table.data.batch.size", 1000);
        int maxCount = intEnv("table.data.batch.max", -1);
        String transactionFile = Option.of(env("transaction.file")).getOrElse("/tmp/copy_db_trans.log");
        return new Config(
                sUrl, sUser, sPassword,
                tUrl, tUser, tPassword,
                tableFilter, batchSize, maxCount, transactionFile);
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

    public String getTransactionFile() {
        return transactionFile;
    }
}
