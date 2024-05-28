package com.firestore;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.FirestoreOptions;
import java.util.Map;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.cloud.firestore.CollectionReference;
import com.google.gson.JsonSyntaxException;

public class pubsubtoFire {
    public static void main(String[] args) {
        Utils options = PipelineOptionsFactory.fromArgs(args).as(Utils.class); // Create pipeline options from the provided arguments
        Pipeline pipeline = Pipeline.create(options);                          // Initialize the pipeline with the created options
        
        String projectId=options.getProjectId().get();                         // Extract necessary configuration values from the options
        String databaseName=options.getDatabaseId().get();
        String collectionName=options.getCollectionName().get();
        String subscriptionName=options.getSubscriptionName().get();

        PCollection<String> messages = pipeline.apply("ReadFromPubSub",
                PubsubIO.readStrings().fromSubscription("projects/" + projectId + "/subscriptions/" + subscriptionName));    // Read messages from the specified Pub/Sub subscription

        messages.apply("ProcessAndSendToFirestore", ParDo.of(new ProcessAndSendToFirestoreFn(projectId,databaseName,collectionName)));    // Process each message and send it to Firestore
        pipeline.run();      // Run the pipeline
    }
    static class ProcessAndSendToFirestoreFn extends DoFn<String, Void> {    // Define a DoFn class to process messages and send them to Firestore
        private final String projectId,databaseName,collectionName;

        ProcessAndSendToFirestoreFn(String projectId,String databaseName,String collectionName) {    // Constructor to initialize Firestore configuration
            this.projectId=projectId;
            this.databaseName=databaseName;
            this.collectionName=collectionName;
        }
        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            FirestoreOptions firestoreOptions = FirestoreOptions.getDefaultInstance().toBuilder()
                    .setProjectId(projectId)
                    .setDatabaseId(databaseName)
                    .build();    // Create Firestore client with the specified project and database IDs
            Firestore firestore = firestoreOptions.getService();
         String message = c.element();                                            // Retrieve the message from the previous pipeline
        CollectionReference collection = firestore.collection(collectionName);    // Reference the Firestore collection
        String[] jsonFormat=message.split(",");                                   // Split and reassemble the message to ensure proper JSON formatting
        String jsonText=String.join(",",jsonFormat);
        Gson gson=new Gson();                                                     // Initialize Gson for JSON parsing
        try{
            try{
                TypeToken<Map<String, Object>> typeToken = new TypeToken<Map<String, Object>>() {};    // Deserialize the JSON message to a map
                Map<String, Object> document = gson.fromJson(message, typeToken.getType());
                if (document.containsKey("after")) {                                                   // Check if the 'after' field exists and process it
                    String afterString = document.get("after").toString();
                    Map<String, Object> afterMap = gson.fromJson(afterString, typeToken.getType());
                    collection.add(afterMap);                               // Add the processed document to Firestore
                } else {        
                    System.out.println("The message does not contain an 'after' field.");
                }
            }catch(JsonSyntaxException e)
            {
                System.out.println("Error occurred while deserializing JSON data: " + e.getMessage());
            }
        }finally
        {
            firestore.close();                // Ensure Firestore client is closed after processing
        }
    }
}
}
