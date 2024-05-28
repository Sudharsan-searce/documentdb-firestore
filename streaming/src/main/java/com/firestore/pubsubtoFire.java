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
        Utils options = PipelineOptionsFactory.fromArgs(args).as(Utils.class);
        Pipeline pipeline = Pipeline.create(options);
        
        String projectId=options.getProjectId().get();
        String databaseName=options.getDatabaseId().get();
        String collectionName=options.getCollectionName().get();
        String subscriptionName=options.getSubscriptionName().get();

        PCollection<String> messages = pipeline.apply("ReadFromPubSub",
                PubsubIO.readStrings().fromSubscription("projects/" + projectId + "/subscriptions/" + subscriptionName));

        messages.apply("ProcessAndSendToFirestore", ParDo.of(new ProcessAndSendToFirestoreFn(projectId,databaseName,collectionName)));
        pipeline.run();
    }
    static class ProcessAndSendToFirestoreFn extends DoFn<String, Void> {
        private final String projectId,databaseName,collectionName;

        ProcessAndSendToFirestoreFn(String projectId,String databaseName,String collectionName) {
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
        String[] jsonFormat=message.split(",");
        String jsonText=String.join(",",jsonFormat);
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
}
