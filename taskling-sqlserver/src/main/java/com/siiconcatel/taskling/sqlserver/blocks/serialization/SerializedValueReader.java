package com.siiconcatel.taskling.sqlserver.blocks.serialization;

import com.siiconcatel.taskling.core.TasklingExecutionException;

import java.sql.ResultSet;
import java.sql.SQLException;

public class SerializedValueReader
{
    public static String readValue(ResultSet rs, String valueColumn, String compressedColum)
    {
        try {
            String value = rs.getString(valueColumn);
            byte[] compressed = rs.getBytes(compressedColum);

            if (value != null) {
                return value;
            } else if (compressed != null) {
                return LargeValueCompressor.unzip(compressed);
            }

            throw new TasklingExecutionException("The both Value and CompressedValue are null");
        }
        catch(SQLException e) {
            throw new TasklingExecutionException("Failed reading item", e);
        }
    }

    public static String readOptionalValue(ResultSet rs, String valueColumn, String compressedColum)
    {
        try {
            String value = rs.getString(valueColumn);
            byte[] compressed = rs.getBytes(compressedColum);

            if (value != null) {
                return value;
            } else if (compressed != null) {
                return LargeValueCompressor.unzip(compressed);
            }

            return null;
        }
        catch(SQLException e) {
            throw new TasklingExecutionException("Failed reading item", e);
        }
    }
}