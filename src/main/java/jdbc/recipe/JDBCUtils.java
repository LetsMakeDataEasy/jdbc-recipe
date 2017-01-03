package jdbc.recipe;

import javaslang.Function1;
import javaslang.collection.AbstractIterator;
import javaslang.collection.Iterator;
import javaslang.collection.List;
import javaslang.collection.Stream;
import jdbc.recipe.model.Column;
import jdbc.recipe.model.IndexColumn;
import jdbc.recipe.model.PrimaryKeyColumn;
import jdbc.recipe.model.Table;

import java.sql.*;

public class JDBCUtils {
    public static Object getResultSetValue(ResultSet rs, int index) throws SQLException {
        Object obj = rs.getObject(index);
        String className = null;
        if (obj != null) {
            className = obj.getClass().getName();
        }
        if (obj instanceof Blob) {
            Blob blob = (Blob) obj;
            obj = blob.getBytes(1, (int) blob.length());
        }
        else if (obj instanceof Clob) {
            Clob clob = (Clob) obj;
            obj = clob.getSubString(1, (int) clob.length());
        }
        else if ("oracle.sql.TIMESTAMP".equals(className) || "oracle.sql.TIMESTAMPTZ".equals(className)) {
            obj = rs.getTimestamp(index);
        }
        else if (className != null && className.startsWith("oracle.sql.DATE")) {
            String metaDataClassName = rs.getMetaData().getColumnClassName(index);
            if ("java.sql.Timestamp".equals(metaDataClassName) || "oracle.sql.TIMESTAMP".equals(metaDataClassName)) {
                obj = rs.getTimestamp(index);
            }
            else {
                obj = rs.getDate(index);
            }
        }
        else if (obj != null && obj instanceof java.sql.Date) {
            if ("java.sql.Timestamp".equals(rs.getMetaData().getColumnClassName(index))) {
                obj = rs.getTimestamp(index);
            }
        }
        return obj;
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

    public static List<Column> loadColumns(DatabaseMetaData metaData, String tableName) throws SQLException {
        try (ResultSet rs = metaData.getColumns(null, null, tableName, "%")) {
            return streamRS(rs).map(Column::fromResultSet).toList();
        }
    }

    public static List<IndexColumn> loadIndexColumns(DatabaseMetaData metaData, String tableName) throws SQLException {
        try (ResultSet rs = metaData.getIndexInfo(null, null, tableName, false, false)) {
            return streamRS(rs).map(IndexColumn::fromResultSet).toList();
        }
    }

    public static List<PrimaryKeyColumn> loadPrimaryKeys(DatabaseMetaData metaData, String tableName) {
        try (ResultSet rs = metaData.getPrimaryKeys(null, null, tableName)) {
            return streamRS(rs).map(PrimaryKeyColumn::fromResultSet).toList();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static <T> List<T> iterateRs(ResultSet rs, Function1<ResultSet, T> rowMapper) {
        return streamRS(rs).map(rowMapper).toList();
    }

    public static List<Table> loadAllTables(DatabaseMetaData metaData) throws SQLException{
        try (ResultSet tableResult = metaData.getTables(null, null, "%", new String[]{"TABLE"})) {
            return iterateRs(tableResult, Table::fromResultSet);
        }
    }
}
