package com.firestore;

import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.FirestoreOptions;
import java.util.Map;
import com.google.gson.Gson;
import org.apache.beam.sdk.transforms.DoFn;
import com.google.gson.reflect.TypeToken;
import com.google.cloud.firestore.CollectionReference;
import com.google.gson.JsonSyntaxException;

public class WriteToFirestoreFn extends DoFn<String, Void> {
    private final String projectId,databaseName,collectionName;

    WriteToFirestoreFn(String projectId,String databaseName,String collectionName) {
        this.projectId=projectId;
        this.databaseName=databaseName;
        this.collectionName=collectionName;
    }
    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
        FirestoreOptions firestoreOptions = FirestoreOptions.getDefaultInstance().toBuilder()
                .setProjectId(projectId)
                .setDatabaseId(databaseName)
                .build();
        Firestore firestore = firestoreOptions.getService();
     String message = c.element();  
    CollectionReference collection = firestore.collection(collectionName);
    // String[] jsonFormat=message.split(",");
    // String jsonText=String.join(",",jsonFormat);
    Gson gson=new Gson();
    try{
        try{
            TypeToken<Map<String, Object>> typeToken = new TypeToken<Map<String, Object>>() {};
            Map<String, Object> document = gson.fromJson(message, typeToken.getType());
            if (document.containsKey("after")) {
                String afterString = document.get("after").toString();
                Map<String, Object> afterMap = gson.fromJson(afterString, typeToken.getType());
                collection.add(afterMap);
            } else {
                System.out.println("The message does not contain an 'after' field.");
            }
        }catch(JsonSyntaxException e)
        {
            System.out.println("Error occurred while deserializing JSON data: " + e.getMessage());
        }
    }finally
    {
        firestore.close();
    }
}
}

