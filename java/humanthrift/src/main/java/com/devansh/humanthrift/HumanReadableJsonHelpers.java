package com.devansh.humanthrift;

import org.json.JSONArray;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;


public class HumanReadableJsonHelpers {

    public static JSONArray readAllFiles(String jsonMetadataPath) {
        File[] jsonFiles = new File[0];
        try {
            jsonFiles = new File(jsonMetadataPath).listFiles();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        JSONArray jsonArray = new JSONArray();

        for (File file : jsonFiles) {
            try (BufferedReader br = new BufferedReader(new FileReader(file.toPath().toString()))) {
                String s = "";
                while (br.ready()) {
                    s += br.readLine() + "\n";
                }
                jsonArray.put(new JSONObject(s));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return jsonArray;
    }
}
