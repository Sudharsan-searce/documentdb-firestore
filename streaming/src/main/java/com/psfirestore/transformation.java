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

import com.google.cloud.firestore.CollectionReference;
import com.google.cloud.firestore.DocumentReference;
import com.google.cloud.firestore.SetOptions;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Map;


public class transformation {

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(options);
        String projectId = "cloudside-academy";
        String topicName = "livedata_bigquery-sub";

        PCollection<String> messages = pipeline.apply("ReadFromPubSub",
                PubsubIO.readStrings().fromSubscription("projects/" + projectId + "/subscriptions/" + topicName));
        messages.apply("TransformAndSendToFirestore", ParDo.of(new ProcessAndSendToFirestoreFn()));
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
            Gson gson = new Gson();
            try {
                TypeToken<Map<String, Object>> typeToken = new TypeToken<Map<String, Object>>() {};
                Map<String, Object> documentData = gson.fromJson(message, typeToken.getType());

                String timestamp = getCurrentTimestamp();
                DocumentReference document = collection.document(timestamp);
                
                // Transform annual salary to monthly salary
                double annualSalary = (double) documentData.get("Annual salary");
                int monthlySalary = (int) (annualSalary / 12.0);
                documentData.put("Monthly salary", monthlySalary);

                // Set the document data with server timestamp
                document.set(documentData, SetOptions.merge());

            } catch (JsonSyntaxException e) {
                System.out.println("Error occurred while deserializing JSON data: " + e.getMessage());
            } finally {
                firestore.close();
            }
        }
    }

    private static String getCurrentTimestamp() {
        LocalDateTime now = LocalDateTime.now(ZoneId.of("Asia/Kolkata"));
        // Format the LocalDateTime as a string
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        return now.format(formatter);
    }
}
