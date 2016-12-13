package jdbc.recipe.model;

import javaslang.control.Option;

import java.sql.ResultSet;
import java.sql.SQLException;

public class PrimaryKeyColumn {
    public Option<String> tableCatalog;
    public Option<String> tableSchema;
    public String tableName;
    public String columnName;
    public int keySeq;
    public String pkName;

    public static PrimaryKeyColumn fromResultSet(ResultSet rs) {
        PrimaryKeyColumn primaryKeyColumn = new PrimaryKeyColumn();
        try {
            primaryKeyColumn.tableCatalog = Option.of(rs.getString("TABLE_CAT"));
            primaryKeyColumn.tableSchema = Option.of(rs.getString("TABLE_SCHEM"));
            primaryKeyColumn.tableName = rs.getString("TABLE_NAME");
            primaryKeyColumn.columnName = rs.getString("COLUMN_NAME");
            primaryKeyColumn.keySeq = rs.getInt("KEY_SEQ");
            primaryKeyColumn.pkName = rs.getString("PK_NAME");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return primaryKeyColumn;
    }
}
