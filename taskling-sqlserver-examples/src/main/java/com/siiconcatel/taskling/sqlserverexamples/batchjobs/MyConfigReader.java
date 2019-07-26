package com.siiconcatel.taskling.sqlserverexamples.batchjobs;

import com.siiconcatel.taskling.core.configuration.TasklingConfigReader;

import java.io.*;
import java.net.URL;
import java.text.MessageFormat;
import java.util.Map;

public class MyConfigReader implements TasklingConfigReader {

    @Override
    public String getTaskConfiguration(String applicationName, String taskName) {
        return loadConfig(applicationName+"_"+taskName+".json");
    }

    private String loadConfig(String fileName) {
        //File file = getFileFromResources(fileName);
        File file = new File("C:/Temp/Taskling/"+fileName);

        try(BufferedReader br = new BufferedReader(new FileReader(file))) {
            StringBuilder sb = new StringBuilder();
            String line = br.readLine();

            while (line != null) {
                sb.append(line);
                sb.append(System.lineSeparator());
                line = br.readLine();
            }
            return sb.toString();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            throw new RuntimeException("No file!");
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("Couldn't read the file!");
        }
    }

    private File getFileFromResources(String fileName) {

        URL resource = ClassLoader.getSystemClassLoader().getResource(fileName);
        if (resource == null) {
            throw new IllegalArgumentException("file is not found!");
        } else {
            return new File(resource.getFile());
        }

    }
}
