package com.firestore;

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
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.cloud.firestore.CollectionReference;
import com.google.gson.JsonSyntaxException;

public class pubsubtoFire {
    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(options);
        String projectId = "gcp-firestore-423907";
        String subName = "doc_stream.streaming.streamfrombatch-sub";
        PCollection<String> messages = pipeline.apply("ReadFromPubSub",
                PubsubIO.readStrings().fromSubscription("projects/" + projectId + "/subscriptions/" + subName));

       messages.apply("ProcessAndSendToFirestore", ParDo.of(new ProcessAndSendToFirestoreFn()));
        pipeline.run();
    }
    static class ProcessAndSendToFirestoreFn extends DoFn<String, Void> {
        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            FirestoreOptions firestoreOptions = FirestoreOptions.getDefaultInstance().toBuilder()
                    .setProjectId("gcp-firestore-423907")
                    .setDatabaseId("firestore-poc")
                    .build();
            Firestore firestore = firestoreOptions.getService();
         String message = c.element();  
        CollectionReference collection = firestore.collection("stream_check");
        String[] jsonFormat=message.split(",");
        String jsonText=String.join(",",jsonFormat);
        Gson gson=new Gson();
        try{
            try{
                TypeToken<Map<String, Object>> typeToken = new TypeToken<Map<String, Object>>() {};
                Map<String, Object> document1 = gson.fromJson(message, typeToken.getType());
                if (document1.containsKey("after")) {
                    String afterString = document1.get("after").toString();
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
