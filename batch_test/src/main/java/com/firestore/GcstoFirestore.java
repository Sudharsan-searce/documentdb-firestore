package com.firestore;


import org.apache.beam.sdk.transforms.DoFn;
import java.io.IOException;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.FirestoreOptions;
import com.google.cloud.firestore.CollectionReference;
import com.google.cloud.firestore.WriteBatch;
import com.google.cloud.firestore.DocumentReference;
import com.google.gson.Gson;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class GcstoFirestore extends DoFn<String, String> {
    private static final int BATCH_SIZE = 1000; // Firestore's max batch size
    // Number of lines to read in each chunk

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
                // Initialize Firestore
        Firestore firestore = FirestoreOptions.getDefaultInstance()
            .toBuilder()
            .setProjectId("gcp-firestore-423907")
            .setDatabaseId("firestore-poc")
            .build()
            .getService();

        CollectionReference collection = firestore.collection("metadata");
        Gson gson = new Gson();

        List<String> jsonLines = new ArrayList<>();
        String data=c.element();
        jsonLines.add(data);

       
         processChunk(jsonLines, collection, gson);
         firestore.shutdown();
         
         try {
            firestore.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
         
        
    }

    private void processChunk(List<String> jsonLines, CollectionReference collection, Gson gson) throws IOException, InterruptedException, ExecutionException {
        List<WriteBatch> batches = new ArrayList<>();
        WriteBatch batch = collection.getFirestore().batch();
        int count = 0;

        for (String json : jsonLines) {
            // Parse each JSON object individually
            Map<String, Object> document = gson.fromJson(json, Map.class);
            // document.remove("versions");
            // document.remove("authors_parsed");

            // Create a new document reference
            DocumentReference docRef = collection.document();
            batch.set(docRef, document);
            count++;

            // If batch size limit is reached, create a new batch
            if (count == BATCH_SIZE) {
                batches.add(batch);
                batch = collection.getFirestore().batch();
                count = 0;
            }
        }

        // Add the last batch if it's not empty
        if (count > 0) {
            batches.add(batch);
        }

        // Commit all batches
        for (WriteBatch wb : batches) {
            wb.commit().get();
        }
        
        
    }
    
}
