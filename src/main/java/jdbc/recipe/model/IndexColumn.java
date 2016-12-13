package jdbc.recipe.model;

import javaslang.control.Option;

import java.sql.ResultSet;
import java.sql.SQLException;

public class IndexColumn {
    public Option<String> tableCatalog;
    public Option<String> tableSchema;
    public String tableName;
    public boolean nonUnique;
    public Option<String> indexQualifier;
    public String indexName;
    public short type;
    public int ordinalPosition;
    public Option<String> columnName; //null when type is tableIndexStatistic
    public Option<Boolean> isAscending;
    public long cardinality;
    public long pages;
    public String filterCondition;

    public static IndexColumn fromResultSet(ResultSet rs) {
        IndexColumn indexColumn = new IndexColumn();
        try {
            indexColumn.tableCatalog = Option.of(rs.getString("TABLE_CAT"));
            indexColumn.tableSchema = Option.of(rs.getString("TABLE_SCHEM"));
            indexColumn.tableName = rs.getString("TABLE_NAME");
            indexColumn.nonUnique = rs.getBoolean("NON_UNIQUE");
            indexColumn.indexQualifier = Option.of(rs.getString("INDEX_QUALIFIER"));
            indexColumn.indexName = rs.getString("INDEX_NAME");
            indexColumn.type = rs.getShort("TYPE");
            indexColumn.ordinalPosition = rs.getInt("ORDINAL_POSITION");
            indexColumn.columnName = Option.of(rs.getString("COLUMN_NAME"));
            indexColumn.isAscending = Option.of(rs.getString("ASC_OR_DESC")).map("A"::equalsIgnoreCase);
            indexColumn.cardinality = rs.getLong("CARDINALITY");
            indexColumn.pages = rs.getLong("PAGES");
            indexColumn.filterCondition = rs.getString("FILTER_CONDITION");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return indexColumn;
    }
}
