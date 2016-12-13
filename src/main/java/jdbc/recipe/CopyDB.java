package jdbc.recipe;

import javaslang.*;
import javaslang.collection.*;
import javaslang.concurrent.Future;
import javaslang.control.Try;
import jdbc.recipe.model.Column;
import jdbc.recipe.model.IndexColumn;
import jdbc.recipe.model.PrimaryKeyColumn;
import jdbc.recipe.model.Table;

import java.sql.*;
import java.text.MessageFormat;

public class CopyDB {

    public static final long NUM_OF_PARALLEL = 4L;

    public static void main(String[] args) throws SQLException {
        Config config = fromEnv();
        doCopy(config);
    }

    private static Config fromEnv() {
        String sUrl = env("source.url");
        String sUser = env("source.user");
        String sPassword = env("source.password");
        String tUrl = env("target.url");
        String tUser = env("target.user");
        String tPassword = env("target.password");
        String tableFilter = env("table.filter");
        int batchSize = intEnv("table.data.batch.size", 1000);
        int maxCount = intEnv("table.data.batch.max", -1);
        return new Config(sUrl, sUser, sPassword, tUrl, tUser, tPassword, tableFilter, batchSize, maxCount);
    }

    private static int intEnv(String key, int defaultValue) {
        return Try.of(() -> Integer.valueOf(env(key))).getOrElse(defaultValue);
    }

    private static String env(String source_url) {
        return System.getenv(source_url);
    }

    private static void doCopy(Config config) throws SQLException {
        try (Connection sConn = getConnection(config.getSrcUrl(), config.getSrcUser(), config.getSrcPassword());
             Connection tConn = getConnection(config.getTargetUrl(), config.getTargetUser(), config.getTargetPassword())) {
            DatabaseMetaData metaData = sConn.getMetaData();
            try (ResultSet tableResult = metaData.getTables(null, null, config.getTableFilter(), new String[]{"TABLE"})) {
                iterateRs(tableResult, Table::fromResultSet)
                        .filter(t -> !unusedTables.contains(t.TABLE_NAME))
                        .map(table -> {
                            String tableName = table.TABLE_NAME;
                            List<Column> columns = getColumns(metaData, tableName);
                            List<PrimaryKeyColumn> primaryKeys = getPrimaryKeys(metaData, tableName);
                            Set<String> pkNames = primaryKeys.map(c -> c.pkName).toSet();
                            List<IndexColumn> indexColumns =
                                    getIndexColumns(metaData, tableName)
                                            .filter(c -> c.indexName != null)
                                            .filter(c -> !pkNames.contains(c.indexName));
                            List<String> tableStructureSql =
                                    Stream.of(dropTableSql(tableName), createTableSql(tableName, columns))
                                    .appendAll(createPrimaryKeySqls(tableName, primaryKeys))
                                    .appendAll(createIndexSqls(tableName, indexColumns))
                                    .toList();
                            return Tuple.of(tableName, tableStructureSql, createInsertSql(tableName, columns));
                        }).toList()
                        .zipWithIndex()
                        .groupBy(t -> t._2 % NUM_OF_PARALLEL)
                        .map(t -> t._2)
                        .map(l -> l.map(t -> t._1))
                        .map(tableGroup -> Future.of(
                                () -> {
                                    tableGroup.forEach(t -> {
                                        String tableName = t._1;
                                        List<String> createTableSql = t._2;
                                        String insertSql = t._3;
                                        executeSqls(tConn, createTableSql);
                                        copyData(sConn, tConn, tableName, insertSql,
                                                config.getBatchSize(), config.getMaxCount());
                                    });
                                    return true;
                                }))
                        .toList()
                        .map(Future::get)
                        .reduce((l, r) -> l && r);
            }
        }
    }

    private static void copyData(
            Connection sourceConnection, Connection targetConnection,
            String tableName, String insertSql, int batchSize, int maxCount) {
        log(insertSql);
        try (PreparedStatement preparedStatement = targetConnection.prepareStatement(insertSql);
             Statement statement = sourceConnection.createStatement()) {
            batchCopyData(statement, preparedStatement,
                    tableName, batchSize, maxCount);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static void batchCopyData(
            Statement sourceStatement, PreparedStatement targetStatement, String tableName, int batchSize, int maxCount) {
        int batchIndex = 0;
        while ((batchSize * batchIndex < maxCount || maxCount == -1) &&
                doBatchCopyData(sourceStatement, targetStatement, tableName, batchSize, batchIndex)) {
            batchIndex++;
        }
    }

    private static boolean doBatchCopyData(
            Statement sourceStatement, PreparedStatement targetStatement,
            String tableName, int batchSize, int batchIndex) {
        String sqlTemplate = "" +
                "SELECT * \n" +
                "  FROM %s \n" +
                "ORDER BY id ASC \n" +
                "OFFSET %s ROWS FETCH NEXT %s ROWS ONLY;\n";
        String sql = String.format(sqlTemplate, tableName, batchIndex * batchSize, batchSize);
        log(sql);
        try (ResultSet rs = sourceStatement.executeQuery(sql)) {
            int rowCount = iterateRs(rs, r -> {
                try {
                    Stream
                            .rangeClosed(1, r.getMetaData().getColumnCount())
                            .forEach(i -> {
                                try {
                                    targetStatement.setObject(i, r.getObject(i));
                                } catch (SQLException e) {
                                    throw new RuntimeException(e);
                                }
                            });
                    targetStatement.addBatch();
                    return 1;
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }).sum().intValue();
            targetStatement.executeBatch();
            targetStatement.clearParameters();
            log(String.format("processed %s rows of table %s", batchIndex * batchSize, tableName));
            return rowCount == batchSize;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static String createInsertSql(String tableName, List<Column> columns) {
        return MessageFormat.format("" +
                        "SET IDENTITY_INSERT {0} ON;\n" +
                        "INSERT INTO {0}({1}) VALUES({2});\n" +
                        "SET IDENTITY_INSERT {0} OFF;",
                tableName,
                columns.map(c -> c.columnName).mkString(","),
                Stream.range(0, columns.length()).map(i -> "?").mkString(","));
    }

    private static String dropTableSql(String tableName) {
        return String.format("" +
                "IF OBJECT_ID('dbo.%s', 'U') IS NOT NULL \n" +
                "DROP TABLE dbo.%s; ", tableName, tableName);
    }

    private static Set<String> unusedTables = Stream.of("dataHandlerLog", "jobBizErrorLog").toSet();

    private static Seq<String> createPrimaryKeySqls(String tableName, List<PrimaryKeyColumn> primaryKeys) {
        String sqlTemplate = "ALTER TABLE %s ADD CONSTRAINT %s PRIMARY KEY CLUSTERED (%s)";
        return primaryKeys.groupBy(pk -> pk.pkName).map(t -> {
            String columnNames = t._2.sortBy(c -> c.keySeq).map(k -> k.columnName).mkString(",");
            String pkName = t._1;
            return String.format(sqlTemplate, tableName, pkName, columnNames);
        });
    }

    private static Seq<String> createIndexSqls(String tableName, List<IndexColumn> indexColumns) {
        String sqlTemplate = "CREATE INDEX [%s] ON %s (%s)";
        return indexColumns
                .groupBy(ic -> ic.indexName)
                .map(t -> {
                    String fieldNames = t._2.sortBy(f -> f.cardinality).map(c -> c.columnName.get()).mkString(",");
                    String indexName = t._1;
                    return String.format(sqlTemplate, indexName, tableName, fieldNames);
                });
    }

    private static void executeSqls(Connection targetConnection, Seq<String> sqls) {
        try (Statement statement = targetConnection.createStatement()) {
            sqls.forEach(sql -> {
                try {
                    log(sql);
                    statement.execute(sql);
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            });
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static String createTableSql(String tableName, List<Column> columns) {
        String fields = columns
                .sortBy(c -> c.ordinalPosition)
                .map(CopyDB::getFieldSpec)
                .mkString(",");
        return String.format("CREATE TABLE %s ( %s )", tableName, fields);
    }

    private static String getFieldSpec(Column c) {
        return String.format("%s %s %s %s",
                c.columnName,
                getDataTypeSpec(c.dataType, c.columnSize, c.decimalDigits),
                c.nullable == Column.ThreeState.NO ? "NOT NULL" : "",
                c.isAutoIncrement == Column.ThreeState.YES ? "IDENTITY" : "");
    }

    private static String getDataTypeSpec(JDBCType dataType, int columnSize, int decimalDigits) {
        switch (dataType) {
            case VARCHAR:
            case NVARCHAR:
                return String.format("%s(%s)", dataType.getName(), columnSize);
            case DECIMAL:
            case NUMERIC:
                return String.format("%s(%s, %s)", dataType.getName(), columnSize, decimalDigits);
            case TIMESTAMP:
                return "DATETIME";
            case CLOB:
                return "VARCHAR(MAX)";
            case BLOB:
                return "VARBINARY(MAX)";
            case DOUBLE:
                return "FLOAT";
            case INTEGER:
                return "INT";
            default:
                return dataType.getName();
        }
    }

    private static List<PrimaryKeyColumn> getPrimaryKeys(DatabaseMetaData metaData, String tableName) {
        try (ResultSet rs = metaData.getPrimaryKeys(null, null, tableName)) {
            return streamRS(rs).map(PrimaryKeyColumn::fromResultSet).toList();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static List<IndexColumn> getIndexColumns(DatabaseMetaData metaData, String tableName) {
        try (ResultSet rs = metaData.getIndexInfo(null, null, tableName, false, false)) {
            return streamRS(rs).map(IndexColumn::fromResultSet).toList();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static List<Column> getColumns(DatabaseMetaData metaData, String tableName) {
        try (ResultSet rs = metaData.getColumns(null, null, tableName, "%")) {
            return streamRS(rs).map(Column::fromResultSet).toList();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static void log(String str) {
        System.out.println(str);
    }

    private static Connection getConnection(String url, String user, String password) throws SQLException {
        return DriverManager.getConnection(url, user, password);
    }

    private static <T> List<T> iterateRs(ResultSet rs, Function1<ResultSet, T> rowMapper) {
        return streamRS(rs).map(rowMapper).toList();
    }

    public static Stream<ResultSet> streamRS(ResultSet rs) {
        return Stream.ofAll(iterableRS(rs));
    }

    public static Iterable<ResultSet> iterableRS(ResultSet rs) {
        return new Iterable<ResultSet>() {
            ResultSet nextRow = null;

            @Override
            public Iterator<ResultSet> iterator() {
                return new AbstractIterator<ResultSet>() {

                    @Override
                    protected ResultSet getNext() {
                        if (nextRow != null || hasNext()) {
                            ResultSet row = nextRow;
                            nextRow = null;
                            return row;
                        }
                        return null;
                    }

                    @Override
                    public boolean hasNext() {
                        if (nextRow != null) {
                            return true;
                        } else {
                            try {
                                if (rs.next()) {
                                    nextRow = rs;
                                    return true;
                                }
                                return false;
                            } catch (SQLException e) {
                                throw new RuntimeException(e);
                            }
                        }
                    }
                };
            }
        };
    }
}
