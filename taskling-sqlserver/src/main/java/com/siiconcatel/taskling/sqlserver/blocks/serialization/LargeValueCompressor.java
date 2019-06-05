package com.siiconcatel.taskling.sqlserver.blocks.serialization;

import com.siiconcatel.taskling.core.TasklingExecutionException;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class LargeValueCompressor {

    public static byte[] zip(String data)
    {
        if (data == null || data.length() == 0)
            return new byte[0];

        try(ByteArrayOutputStream bos = new ByteArrayOutputStream(data.length())) {
            try(GZIPOutputStream gzip = new GZIPOutputStream(bos)) {
                gzip.write(data.getBytes(StandardCharsets.UTF_8));
            }
            byte[] compressed = bos.toByteArray();
            return compressed;
        }
        catch(IOException e) {
            throw new TasklingExecutionException("Failed zipping value", e);
        }
    }

    public static String unzip(byte[] compressed)
    {
        if(compressed == null || compressed.length == 0)
            return "";

        try(ByteArrayInputStream bis = new ByteArrayInputStream(compressed)) {
            try(GZIPInputStream gis = new GZIPInputStream(bis)) {
                try(BufferedReader br = new BufferedReader(new InputStreamReader(gis, StandardCharsets.UTF_8))) {
                    StringBuilder sb = new StringBuilder();
                    String line;
                    while ((line = br.readLine()) != null) {
                        sb.append(line);
                    }

                    return sb.toString();
                }
            }
        }
        catch(IOException e) {
            throw new TasklingExecutionException("Failed unzipping value", e);
        }
    }
}
