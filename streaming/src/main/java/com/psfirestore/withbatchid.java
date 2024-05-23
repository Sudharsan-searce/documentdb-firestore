package com.psfirestore;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.FirestoreOptions;
import java.util.Map;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.cloud.firestore.CollectionReference;
import com.google.cloud.firestore.DocumentReference;
import com.google.gson.JsonSyntaxException;
import java.util.UUID; // Import UUID for generating unique IDs

public class withbatchid {

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(options);
        String projectId = "cloudside-academy";
        String topicName = "livedata_bigquery-sub";

        PCollection<String> messages = pipeline.apply("ReadFromPubSub",
                PubsubIO.readStrings().fromSubscription("projects/" + projectId + "/subscriptions/" + topicName));
        messages.apply("ProcessAndSendToFirestore", ParDo.of(new ProcessAndSendToFirestoreFn()));
        pipeline.run();
    }

    static class ProcessAndSendToFirestoreFn extends DoFn<String, Void> {
        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            FirestoreOptions firestoreOptions = FirestoreOptions.getDefaultInstance().toBuilder()
                    .setProjectId("cloudside-academy")
                    .build();
            Firestore firestore = firestoreOptions.getService();
            String message = c.element();
            CollectionReference collection = firestore.collection("details_11");
            String[] jsonFormat=message.split(",");
            String jsonText=String.join(",",jsonFormat);
            Gson gson=new Gson();
            try {
                TypeToken<Map<String, Object>> typeToken = new TypeToken<Map<String, Object>>() {};
                Map<String, Object> documentData = gson.fromJson(jsonText, typeToken.getType());

                // Generate unique document ID using UUID
                String batchId = UUID.randomUUID().toString();

                // Set the document with the generated ID
                DocumentReference document = collection.document(batchId);

                // Add the document data to Firestore
                document.set(documentData);
                
            } catch(JsonSyntaxException e) {
                System.out.println("Error occurred while deserializing JSON data: " + e.getMessage());
            } finally {
                firestore.close();
            }
        }
    }
}

