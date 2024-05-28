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
   
    private final String pro_id,db_name,collection_name,batch_size;


    public GcstoFirestore(String pro_id,String db_name,String collection_name,String batch_size) {
        this.pro_id=pro_id;
        this.db_name=db_name;
        this.collection_name=collection_name;
        this.batch_size=batch_size;
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
                // Initialize Firestore
        Firestore firestore = FirestoreOptions.getDefaultInstance()
            .toBuilder()
            .setProjectId(pro_id)
            .setDatabaseId(db_name)
            .build()
            .getService();
        
        CollectionReference collection = firestore.collection(collection_name);
        Gson gson = new Gson();

        List<String> jsonLines = new ArrayList<>();
        String data=c.element(); // Input from previous pipeline
        

       // batch_processing
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
            

            // Create a new document reference
            DocumentReference docRef = collection.document();
            batch.set(docRef, document);
            count++;

            // If batch size limit is reached, create a new batch
            if (count == Integer.parseInt(batch_size)) {
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
