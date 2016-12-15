package jdbc.recipe;

import javaslang.*;
import javaslang.collection.*;
import javaslang.concurrent.Future;
import javaslang.control.Option;
import javaslang.control.Try;
import jdbc.recipe.model.Column;
import jdbc.recipe.model.IndexColumn;
import jdbc.recipe.model.PrimaryKeyColumn;
import jdbc.recipe.model.Table;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.*;
import java.text.MessageFormat;
import java.util.stream.Collectors;

import static java.nio.file.StandardOpenOption.*;

public class CopyDB {

    private static final long NUM_OF_PARALLEL = 4L;
    private static Set<String> unusedTables = Stream.of(
            "dataHandlerLog", "jobBizErrorLog", "emailAlerted",
            "emailAlert").toSet();

    public static void main(String[] args) throws Exception {
        Config config = Config.fromEnv();
        try (Connection sConn = getConnection(config.getSrcUrl(), config.getSrcUser(), config.getSrcPassword());
             Connection tConn = getConnection(config.getTargetUrl(), config.getTargetUser(), config.getTargetPassword());
        ) {
            Path path = FileSystems.getDefault().getPath(config.getTransactionFile());
            Map<String, Long> startedTables = recoverFromTranslog(tConn, path, config.getTableFilter());

            DatabaseMetaData metaData = sConn.getMetaData();
            Try.of(() -> getTableList(metaData, config.getTableFilter()))
                    .andThenTry(tables -> copyTables(config, sConn, tConn, path, startedTables, metaData, tables));
        }
    }

    private static List<Table> getTableList(DatabaseMetaData metaData, String tableFilter) throws SQLException {
        try (ResultSet tableResult = metaData.getTables(null, null, tableFilter, new String[]{"TABLE"})) {
            return iterateRs(tableResult, Table::fromResultSet);
        }
    }

    private static Boolean copyTables(Config config, Connection sConn, Connection tConn, Path path, Map<String, Long> startedTables, DatabaseMetaData metaData, List<Table> tables) {
        return tables
                .filter(t1 -> !unusedTables.contains(t1.TABLE_NAME))
                .map(table -> {
                    String tableName = table.TABLE_NAME;
                    Tuple3<List<Column>, List<PrimaryKeyColumn>, List<IndexColumn>> tableInfo = loadTableInfo(metaData, tableName);
                    return (Function0<Boolean>) () -> copyOneTable(config, sConn, tConn, path, startedTables, tableName, tableInfo);
                })
                .zipWithIndex()
                .groupBy(t -> t._2 % NUM_OF_PARALLEL)
                .map(t -> t._2)
                .map(l -> l.map(t -> t._1))
                .map(taskGroup ->
                        Future.of(() -> {
                            taskGroup.forEach(Function0::apply);
                            return true;
                        }).onFailure(e -> {
                            throw new RuntimeException(e);
                        }))
                .toList()
                .map(x -> x.getOrElse(false))
                .foldLeft(true, (l, r) -> l && r);
    }

    private static Boolean copyOneTable(Config config, Connection sConn, Connection tConn, Path path, Map<String, Long> startedTables, String tableName, Tuple3<List<Column>, List<PrimaryKeyColumn>, List<IndexColumn>> tableInfo) {
        Option<Long> lastOffset = startedTables.get(tableName);
        boolean isNotCreated = lastOffset.isEmpty();
        if (isNotCreated) {
            createTable(tConn, tableName, tableInfo);
        }
        Option<String> insertSql = createInsertSql(tableName, tableInfo._1);
        if (!insertSql.isEmpty()) {
            copyData(sConn, tConn, tableName, insertSql.get(),
                    config.getBatchSize(), lastOffset, path);
        }
        return true;
    }

    private static void createTable(Connection tConn, String tableName, Tuple3<List<Column>, List<PrimaryKeyColumn>, List<IndexColumn>> tableInfo) {
        List<String> tableStructureSql = makeCreateTableSql(tableName, tableInfo._1, tableInfo._2, tableInfo._3);
        executeSqls(tConn, tableStructureSql);
    }

    private static Tuple3<List<Column>, List<PrimaryKeyColumn>, List<IndexColumn>> loadTableInfo(DatabaseMetaData metaData, String tableName) {
        List<Column> columns = loadColumns(metaData, tableName);
        List<PrimaryKeyColumn> primaryKeys = loadPrimaryKeys(metaData, tableName);
        List<IndexColumn> indexColumnList = loadIndexColumns(metaData, tableName);
        return Tuple.of(columns, primaryKeys, indexColumnList);
    }

    private static List<String> makeCreateTableSql(
            String tableName,
            List<Column> columns,
            List<PrimaryKeyColumn> primaryKeys,
            List<IndexColumn> indexColumnList) {
        Set<String> pkNames = primaryKeys.map(c -> c.pkName).toSet();
        List<IndexColumn> indexColumns = indexColumnList
                .filter(c -> c.indexName != null)
                .filter(c -> !pkNames.contains(c.indexName));
        return Stream.of(dropTableSql(tableName), createTableSql(tableName, columns))
                .appendAll(createPrimaryKeySqls(tableName, primaryKeys))
                .appendAll(createIndexSqls(tableName, indexColumns))
                .toList();
    }

    private static Map<String, Long> recoverFromTranslog(Connection tConn, Path path, String tableFilter) throws IOException {
        log("Using translog from " + path.toString());
        List<Tuple2<String, Long>> translog = getTranslogSnapshot(path);
        executeSqls(tConn,
                translog.filter(t -> t._1.matches(tableFilter.replace("%", ".*")))
                        .filter(t -> !unusedTables.contains(t._1))
                        .map(t1 -> String.format("DELETE FROM %s WHERE id > %s", t1._1, t1._2)));
        return translog.toMap(t -> t);
    }

    private static List<Tuple2<String, Long>> getTranslogSnapshot(Path path) throws IOException {
        if (Files.exists(path)) {
            return Stream.ofAll(Files.lines(path).collect(Collectors.toList()))
                    .map(x -> {
                        String[] kv = x.split("=");
                        return Tuple.of(kv[0], kv[1]);
                    })
                    .groupBy(t -> t._1)
                    .map(t -> Tuple.of(t._1, t._2.map(x -> Long.valueOf(x._2)).max().get()))
                    .toList();
        }
        return List.empty();
    }

    private static void copyData(
            Connection sourceConnection, Connection targetConnection,
            String tableName, String insertSql, int batchSize,
            Option<Long> startOffset, Path translogPath) {
        log(insertSql);
        try (PreparedStatement preparedStatement = targetConnection.prepareStatement(insertSql);
             Statement statement = sourceConnection.createStatement()) {
            ResultSet resultSet = statement.executeQuery("SELECT max(id) FROM " + tableName);
            resultSet.next();
            long maxId = resultSet.getLong(1);
            batchCopyData(statement, preparedStatement,
                    tableName, batchSize, maxId, startOffset, translogPath);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static void batchCopyData(
            Statement sourceStatement, PreparedStatement targetStatement,
            String tableName, int batchSize, long maxId,
            Option<Long> startOffset, Path translogPath) {
        int offset = Math.toIntExact(startOffset.getOrElse(0L));
        while ((offset < maxId)) {
            doBatchCopyData(sourceStatement, targetStatement, tableName, batchSize, offset, translogPath);
            offset += batchSize;
        }
    }

    private static void appendTranslog(Path translogPath, String tableName, int position) {
        try {
            Files.write(translogPath, List.of(String.format("%s=%s", tableName, position)), APPEND, WRITE, CREATE);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static boolean doBatchCopyData(
            Statement sourceStatement, PreparedStatement targetStatement,
            String tableName, int batchSize, int offset, Path translogPath) {
        String sqlTemplate = "" +
                "SELECT * \n" +
                "  FROM %s \n" +
                " WHERE id >= %s and id < %s;\n";
        String sql = String.format(sqlTemplate, tableName, offset, offset + batchSize);
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
            log(String.format("processed %s rows of table %s", offset + rowCount, tableName));
            appendTranslog(translogPath, tableName, offset);
            return true;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static Option<String> createInsertSql(String tableName, List<Column> columns) {
        Option<Column> idColumn = columns.find(c -> c.columnName.equalsIgnoreCase("id"));
        if (idColumn.isEmpty()) {
            return Option.none();
        }
        String insertSql = MessageFormat.format("INSERT INTO {0}({1}) VALUES({2})",
                tableName,
                columns.map(c -> c.columnName).mkString(","),
                Stream.range(0, columns.length()).map(i -> "?").mkString(","));
        if (idColumn.get().isAutoIncrement == Column.ThreeState.YES) {
            return Option.some(MessageFormat.format("" +
                            "SET IDENTITY_INSERT {0} ON;\n" + "{1};\n" +
                            "SET IDENTITY_INSERT {0} OFF;\n",
                    tableName,
                    insertSql));
        } else {
            return Option.some(insertSql);
        }
    }

    private static String dropTableSql(String tableName) {
        return String.format("" +
                "IF OBJECT_ID('dbo.%s', 'U') IS NOT NULL \n" +
                "DROP TABLE dbo.%s; ", tableName, tableName);
    }

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

    private static List<PrimaryKeyColumn> loadPrimaryKeys(DatabaseMetaData metaData, String tableName) {
        try (ResultSet rs = metaData.getPrimaryKeys(null, null, tableName)) {
            return streamRS(rs).map(PrimaryKeyColumn::fromResultSet).toList();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static List<IndexColumn> loadIndexColumns(DatabaseMetaData metaData, String tableName) {
        try (ResultSet rs = metaData.getIndexInfo(null, null, tableName, false, false)) {
            return streamRS(rs).map(IndexColumn::fromResultSet).toList();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static List<Column> loadColumns(DatabaseMetaData metaData, String tableName) {
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
