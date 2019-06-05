package com.siiconcatel.taskling.sqlserver.ancilliaryservices;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.microsoft.sqlserver.jdbc.ISQLServerBulkRecord;
import com.microsoft.sqlserver.jdbc.SQLServerException;

public class SQLServerBulkListRecord implements ISQLServerBulkRecord {
    protected class ColumnMetadata
    {
        String columnName;
        int columnType;
        int precision;
        int scale;

        ColumnMetadata(String name, int type, int precision, int scale)
        {
            columnName = name;
            columnType = type;
            this.precision = precision;
            this.scale = scale;
        }
    }

    private Object[] currentLine;
    private int currentIndex;
    private List<Object[]> rows;

    private Map<Integer, ColumnMetadata> columnMetadata;

    public SQLServerBulkListRecord(List<Object[]> rows)
    {
        columnMetadata = new HashMap<>();
        currentIndex = 0;
        this.rows = rows;

        if (rows.size() > 0)
            this.currentLine = rows.get(0);
    }

    @Override
    public Set<Integer> getColumnOrdinals()
    {
        return columnMetadata.keySet();
    }

    @Override
    public String getColumnName(int column)
    {
        return columnMetadata.get(column).columnName;
    }

    @Override
    public int getColumnType(int column)
    {
        return columnMetadata.get(column).columnType;
    }

    @Override
    public int getPrecision(int column)
    {
        return columnMetadata.get(column).precision;
    }

    @Override
    public int getScale(int column)
    {
        return columnMetadata.get(column).scale;
    }

    @Override
    public boolean isAutoIncrement(int column)
    {
        return false;
    }

    @Override
    public Object[] getRowData() throws SQLServerException
    {
        return currentLine;
    }

    public List<String> getColumnsNames() {
        return columnMetadata.entrySet()
                .stream()
                .sorted(Map.Entry.comparingByKey())
                .map(x -> x.getValue().columnName)
                .collect(Collectors.toList());
    }

    public void addColumnMetadata(int positionInSource, String name, int jdbcType, int precision, int scale)
    {

        if (positionInSource < 0)
        {
            throw new RuntimeException("Bad col index");
        }

        switch (jdbcType) {
            /*
             * SQL Server supports numerous string literal formats for temporal types, hence sending them as varchar with
             * approximate precision(length) needed to send supported string literals. string literal formats supported by
             * temporal types are available in MSDN page on data types.
             */
            case java.sql.Types.DATE:
            case java.sql.Types.TIME:
            case java.sql.Types.TIMESTAMP:
            case microsoft.sql.Types.DATETIMEOFFSET:
                columnMetadata.put(positionInSource, new ColumnMetadata(name, jdbcType, 50, scale));
                break;

            // Redirect SQLXML as LONGNVARCHAR
            // SQLXML is not valid type in TDS
            case java.sql.Types.SQLXML:
                columnMetadata.put(positionInSource,
                        new ColumnMetadata(name, java.sql.Types.LONGNVARCHAR, precision, scale));
                break;

            // Redirecting Float as Double based on data type mapping
            // https://msdn.microsoft.com/en-us/library/ms378878%28v=sql.110%29.aspx
            case java.sql.Types.FLOAT:
                columnMetadata.put(positionInSource, new ColumnMetadata(name, java.sql.Types.DOUBLE, precision, scale));
                break;

            // redirecting BOOLEAN as BIT
            case java.sql.Types.BOOLEAN:
                columnMetadata.put(positionInSource, new ColumnMetadata(name, java.sql.Types.BIT, precision, scale));
                break;

            default:
                columnMetadata.put(positionInSource, new ColumnMetadata(name, jdbcType, precision, scale));
        }
    }

    @Override
    public boolean next() throws SQLServerException
    {

        if (currentIndex == rows.size())
        {
            currentLine = null;
            return false;
        } else
        {
            currentLine = rows.get(currentIndex);
            currentIndex++;
            return true;
        }
    }
}
