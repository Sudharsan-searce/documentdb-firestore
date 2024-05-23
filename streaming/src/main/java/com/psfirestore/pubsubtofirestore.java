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
import com.google.gson.JsonSyntaxException;

public class pubsubtofirestore {

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
        CollectionReference collection = firestore.collection("details_10");
        String[] jsonFormat=message.split(",");
        String jsonText=String.join(",",jsonFormat);
        Gson gson=new Gson();
        try{

            try{
    
                TypeToken<Map<String, Object>> typeToken = new TypeToken<Map<String, Object>>() {};
                Map<String, Object> document1 = gson.fromJson(jsonText, typeToken.getType());
                System.out.println(document1);
                collection.add(document1);

                // String[] timestamp=(String[]) document1.get("timestamp");
                // String new_timestamp=timestamp[1];
                // document2.put(new_timestamp, new_timestamp);
                // collection.add(document2);

                
                // String dateTimeString=(String) document1.get("timestamp");
                // DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSSX");
                // try {
                //     LocalDateTime datetime = LocalDateTime.parse(dateTimeString, formatter);
                //     String datePart = datetime.toLocalDate().toString();
                //     System.out.println("Extracted Date: " + datePart);
                //     document1.put("extract_date",datePart);
                //     collection.add(document1);
                // } catch (Exception e) {
                //     System.err.println("Error parsing datetime: " + e.getMessage());
                // }
                
         
                
                // double numBytes = (double) document1.get("num_bytes");
                // double numKb = numBytes / 1024.0;
                // document1.put("num_kb", numKb);
                // collection.add(document1);
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