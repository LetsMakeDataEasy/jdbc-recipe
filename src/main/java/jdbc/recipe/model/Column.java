package jdbc.recipe.model;

import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.SQLException;

public class Column {
    public static Column fromResultSet(ResultSet rs) {
        Column column = new Column();
        try {
            column.tableCatalog = rs.getString("TABLE_CAT");
            column.tableSchema = rs.getString("TABLE_SCHEM");
            column.tableName = rs.getString("TABLE_NAME");
            column.columnName = rs.getString("COLUMN_NAME");
            column.dataType = JDBCType.valueOf(rs.getInt("DATA_TYPE"));
            column.typeName = rs.getString("TYPE_NAME");
            column.columnSize = Math.max(rs.getInt("COLUMN_SIZE"), rs.getInt("BUFFER_LENGTH"));
            column.decimalDigits = rs.getInt("DECIMAL_DIGITS");
            column.radix = rs.getInt("NUM_PREC_RADIX");
            column.nullable = getThreeState(rs.getString("IS_NULLABLE"));
            column.remarks = rs.getString("REMARKS");
            column.defaultValue = rs.getString("COLUMN_DEF");
            column.charOctetLength = rs.getInt("CHAR_OCTET_LENGTH");
            column.ordinalPosition = rs.getInt("ORDINAL_POSITION");
            column.isAutoIncrement = getThreeState(rs.getString("IS_AUTOINCREMENT"));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return column;
    }

    public static ThreeState getThreeState(String isNullable) {
        return ThreeState.valueOf(isNullable.isEmpty() ? "empty" : isNullable);
    }

    public enum ThreeState {
        YES, NO, empty;

        public static ThreeState of(String str) {
            switch (str) {
                case "YES":
                    return YES;
                case "NO":
                    return NO;
                default:
                    return empty;
            }
        }
    }

    public String tableCatalog;
    public String tableSchema;
    public String tableName;
    public String columnName;
    public JDBCType dataType;
    public String typeName;
    public int columnSize;
    public int decimalDigits;
    public int radix;
    public ThreeState nullable;
    public String remarks;
    public String defaultValue;
    public int charOctetLength;
    public int ordinalPosition;
    public ThreeState isAutoIncrement;
}
