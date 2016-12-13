package jdbc.recipe.model;

import java.sql.ResultSet;
import java.sql.SQLException;

public class Table {
    public String TABLE_CAT;
    public String TABLE_SCHEM;
    public String TABLE_NAME;
    public String TABLE_TYPE;
    public String REMARKS;
    public String TYPE_CAT;
    public String TYPE_SCHEM;
    public String TYPE_NAME;
    public String SELF_REFERENCING_COL_NAME;
    public String REF_GENERATION;

    public static Table fromResultSet(ResultSet rs) {
        Table table = new Table();
        try {
            table.TABLE_CAT = rs.getString("TABLE_CAT");
            table.TABLE_SCHEM = rs.getString("TABLE_SCHEM");
            table.TABLE_NAME = rs.getString("TABLE_NAME");
            table.TABLE_TYPE = rs.getString("TABLE_TYPE");
            table.REMARKS = rs.getString("REMARKS");
            table.TYPE_CAT = rs.getString("TYPE_CAT");
            table.TYPE_SCHEM = rs.getString("TYPE_SCHEM");
            table.TYPE_NAME = rs.getString("TYPE_NAME");
            table.SELF_REFERENCING_COL_NAME = rs.getString("SELF_REFERENCING_COL_NAME");
            table.REF_GENERATION = rs.getString("REF_GENERATION");

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return table;
    }
}
