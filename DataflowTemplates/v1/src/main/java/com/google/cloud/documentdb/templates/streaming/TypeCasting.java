package com.google.cloud.documentdb.templates.streaming;

import org.json.JSONArray;
import org.json.JSONObject;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import com.google.cloud.Timestamp;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import java.nio.channels.ReadableByteChannel;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.time.Instant;
import java.time.format.DateTimeParseException;


public class TypeCasting {
    private static Map<String, String> typeMapping;

    private static Map<String, String> readTypeMappings(String filePath) throws IOException {
        
        FileSystems.setDefaultPipelineOptions(PipelineOptionsFactory.create());
        MatchResult.Metadata metadata = FileSystems.matchSingleFileSpec(filePath);
        ReadableByteChannel channel = FileSystems.open(metadata.resourceId());
        // Read the file content
        try (InputStream stream = Channels.newInputStream(channel);
             ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            byte[] buffer = new byte[1024];
            int len;
            while ((len = stream.read(buffer)) != -1) {
                baos.write(buffer, 0, len);
            }
            String content = baos.toString(StandardCharsets.UTF_8);
        JSONObject jsonObject = new JSONObject(content);
        JSONObject mappings = jsonObject.getJSONObject("DocumentDB_to_Firestore_Type_Mapping");
        Map<String, String> typeMapping = new HashMap<>();
        for (String key : mappings.keySet()) {
            typeMapping.put(key, mappings.getString(key));
        }
        return typeMapping;
    }
}

    public Map<String, Object> convertDataType(JSONObject jsonData,String typeMappingPath) throws IOException {
        typeMapping = readTypeMappings(typeMappingPath);
        Map<String, Object> data = jsonToMap(cast(jsonData));
        Map<String, Object> typecastedData = typecastData(data, typeMapping);
        return typecastedData;
    }

    private static Map<String, Object> jsonToMap(JSONObject jsonObject) {
        Map<String, Object> map = new HashMap<>();
        for (String key : jsonObject.keySet()) {
            Object value = jsonObject.get(key);
            if (value instanceof JSONObject) {
                value = jsonToMap((JSONObject) value);
            } else if (value instanceof JSONArray) {
                // value = jsonToList((JSONArray) value);
                value=value.toString();
            }
            map.put(key, value);
        }
        return map;
    }

    private static List<Object> jsonToList(JSONArray jsonArray) {
        List<Object> list = new ArrayList<>();
        for (int i = 0; i < jsonArray.length(); i++) {
            Object value = jsonArray.get(i);
            if (value instanceof JSONObject) {
                value = jsonToMap((JSONObject) value);
            } else if (value instanceof JSONArray) {
                value = jsonToList((JSONArray) value);
            }
            list.add(value);
        }
        return list;
    }

    private static Map<String, Object> typecastData(Map<String, Object> data, Map<String, String> typeMapping) {
        Map<String, Object> typecastedData = new HashMap<>();
        for (Map.Entry<String, Object> entry : data.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();

            if (value == null) {
                typecastedData.put(key, null);
                continue;
            }

            String valueType = value.getClass().getSimpleName();
            String targetType = typeMapping.get(valueType);

            if (targetType == null) {
                typecastedData.put(key, value);
                continue;
            }

            switch (targetType) {
                case "string":
                    typecastedData.put(key, value.toString());
                    break;
                case "boolean":
                    typecastedData.put(key, Boolean.valueOf(value.toString()));
                    break;
                // case "timestamp":
                //     typecastedData.put(key, value.toString());
                //     break;
                case "map":
                    if (value instanceof Map) {
                        typecastedData.put(key, typecastData((Map<String, Object>) value, typeMapping));
                    } else {
                        typecastedData.put(key, value.toString());
                    }
                    break;
                case "null":
                    typecastedData.put(key, null);
                    break;
                case "array":
                    typecastedData.put(key, value);
                    break;
                case "bytes":
                    typecastedData.put(key, value.toString().getBytes());
                    break;
                case "number":
                    typecastedData.put(key, Double.valueOf(value.toString()));
                    break;
                default:
                    typecastedData.put(key, value);
                    break;
            }
        }
        return typecastedData;
    }

    public JSONObject cast(JSONObject jsonObject) {
        convertFunctionMapping(jsonObject);
        return jsonObject;
    }

    public static void convertFunctionMapping(Object json) {
        if (json instanceof JSONObject) {
            JSONObject jsonObject = (JSONObject) json;
            Iterator<String> keys = jsonObject.keys();

            while (keys.hasNext()) {
                String key = keys.next();
                Object value = jsonObject.get(key);

                if (value instanceof JSONObject) {
                    JSONObject nestedObject = (JSONObject) value;
                    if (nestedObject.has("$date")) {
                        Object dateValue = nestedObject.get("$date");
                        
                        try {
                            Instant instant;
                            if (dateValue instanceof String) {
                                // Parse the date string to a Date object
                                String dateTime = (String) dateValue;
                                instant = Instant.parse(dateTime);
                            } else if (dateValue instanceof Number) {
                                // Convert epoch time to a Date object
                                long epochTime = ((Number) dateValue).longValue();
                                instant = Instant.ofEpochMilli(epochTime);
                            } else {
                                throw new IllegalArgumentException("Invalid date format");
                            }
                            // Convert Date to Timestamp
                            Timestamp timestamp = Timestamp.of(Date.from(instant));
                            // Add the Timestamp to the original JSON object
                            jsonObject.put(key,timestamp);
                       } catch (DateTimeParseException e) {
                            e.printStackTrace();
                        }   
                      } else if (nestedObject.has("$timestamp")) {
                        JSONObject timestampObject = nestedObject.getJSONObject("$timestamp");
                        long seconds = timestampObject.getLong("t");
                        int nanos = timestampObject.getInt("i") * 1000;
                        long millis = seconds * 1000 + nanos / 1000000;
                        Date date = new Date(millis);
                        jsonObject.put(key, Timestamp.of(date));
                    } else if (nestedObject.has("$numberDecimal")) {
                        String decimalValue = nestedObject.getString("$numberDecimal");
                        jsonObject.put(key, new BigDecimal(decimalValue));
                    }
                    else if (nestedObject.has("$numberLong")) {
                        long decimalValue = nestedObject.getLong("$numberLong");
                        jsonObject.put(key, decimalValue);
                    } 
                    else {
                        convertFunctionMapping(nestedObject);
                    }
                } else if (value instanceof JSONArray) {
                    convertFunctionMapping(value);
                }
            }
        } else if (json instanceof JSONArray) {
            JSONArray jsonArray = (JSONArray) json;
            for (int i = 0; i < jsonArray.length(); i++) {
                Object value = jsonArray.get(i);
                if (value instanceof JSONObject) {
                    JSONObject nestedObject = (JSONObject) value;
                    if (nestedObject.has("$date")) {
                        Object dateValue = nestedObject.get("$date");
                        System.out.print("DateValue :"+dateValue);
                        try {
                            Instant instant;
                            if (dateValue instanceof String) {
                                // Parse the date string to a Date object
                                String dateTime = (String) dateValue;
                                instant = Instant.parse(dateTime);
                            } else if (dateValue instanceof Number) {
                                // Convert epoch time to a Date object
                                long epochTime = ((Number) dateValue).longValue();
                                instant = Instant.ofEpochMilli(epochTime);
                            } else {
                                throw new IllegalArgumentException("Invalid date format");
                            }
                            // Convert Date to Timestamp
                            Timestamp timestamp = Timestamp.of(Date.from(instant));
                            // Add the Timestamp to the original JSON object
                            jsonArray.put(i,timestamp);
                       } catch (DateTimeParseException e) {
                            e.printStackTrace();
                        } 
                    } else if (nestedObject.has("$timestamp")) {
                        JSONObject timestampObject = nestedObject.getJSONObject("$timestamp");
                        long seconds = timestampObject.getLong("t");
                        int nanos = timestampObject.getInt("i") * 1000;
                        long millis = seconds * 1000 + nanos / 1000000;
                        SimpleDateFormat sdf = new SimpleDateFormat("MMMM d, yyyy 'at' hh:mm:ss.SSS a z");
                        Date date = new Date(millis);
                        jsonArray.put(i, sdf.format(date));
                    } else if (nestedObject.has("$numberDecimal")) {
                        String decimalValue = nestedObject.getString("$numberDecimal");
                        jsonArray.put(i, new BigDecimal(decimalValue));
                    }
                    else if (nestedObject.has("$numberLong")) {
                        long decimalValue = nestedObject.getLong("$numberLong");
                        jsonArray.put(i, decimalValue);
                    }
                     else {
                        convertFunctionMapping(nestedObject);
                    }
                } else if (value instanceof JSONArray) {
                    convertFunctionMapping(value);
                }
            }
        }
    }
    
}

