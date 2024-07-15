package com.google.cloud.documentdb.templates.streaming;

import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.FirestoreOptions;
import com.google.cloud.firestore.QueryDocumentSnapshot;
import com.google.cloud.firestore.CollectionReference;
import com.google.cloud.firestore.DocumentReference;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.gson.JsonSyntaxException;
import com.google.api.core.ApiFuture;
import com.google.cloud.firestore.QuerySnapshot;
import java.util.List;
import com.google.pubsub.v1.PubsubMessage;
import java.util.Map;
import org.apache.beam.sdk.transforms.DoFn;
import org.json.JSONObject;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;


public class PubsubToFirestore extends DoFn<PubsubMessage, Void> {
    private final String projectId, databaseName, collectionName,typeMappingPath;
    PubsubToFirestore(String projectId, String databaseName, String collectionName,String typeMappingPath) {
        this.projectId = projectId;
        this.databaseName = databaseName;
        this.collectionName = collectionName;
        this.typeMappingPath=typeMappingPath;
    }
    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
        Firestore firestore = null;
        TypeCasting typecasting=new TypeCasting();
        String eventData;
        JSONObject jsonData;
        Map<String, Object> event;
        // PubsubMessage message=null;
        TypeToken<Map<String, Object>> typeToken = new TypeToken<Map<String, Object>>() {
        };
        Gson gson = new Gson();
        try {
            FirestoreOptions firestoreOptions = FirestoreOptions.getDefaultInstance().toBuilder()
            .setProjectId(projectId).setDatabaseId(databaseName).build();
            firestore = firestoreOptions.getService();
            if (firestore == null) {
                System.err.println("Failed to initialize Firestore. Firestore is null.");
                return;
            }
            PubsubMessage message = c.element();
            System.out.print("Data : "+message);
            String orderingKeyJsonString = extractJsonString(message.toString(), "ordering_key");
            ObjectMapper mapper = new ObjectMapper();
            orderingKeyJsonString = orderingKeyJsonString.replace("\\\"", "\"").replace("\\\\", "\\");
            System.out.println("OID 2 STRING: " + orderingKeyJsonString);
            // Parse the cleaned ordering_key JSON string
            JsonNode orderingKeyNode = mapper.readTree(orderingKeyJsonString);
            // Extract the payload.id field
            String payloadId = orderingKeyNode.path("payload").path("id").asText();
            JsonNode oidNode = mapper.readTree(payloadId);
            String oid_str=oidNode.path("$oid").asText();
            System.out.println("Extracted OIDDDDDDDDDDDD: " + oid_str);
            CollectionReference collection = firestore.collection(collectionName);
            try {
                Map<String, Object> pubsubMessage = gson.fromJson(message.getData().toStringUtf8(), typeToken.getType());
                switch (pubsubMessage.get("op").toString()) {
                    case "c": // for create event
                            eventData = pubsubMessage.get("after").toString();
                            jsonData = new JSONObject(eventData);
                            event = typecasting.convertDataType(jsonData,typeMappingPath);
                            collection.add(event);
                            break;

                    case "r": // for read event
                            eventData = pubsubMessage.get("after").toString();
                            jsonData = new JSONObject(eventData);
                            event = typecasting.convertDataType(jsonData,typeMappingPath);
                            collection.add(event);
                            break;
                    case "u": // for update event
                            eventData = pubsubMessage.get("after").toString();
                            jsonData = new JSONObject(eventData);
                            event = typecasting.convertDataType(jsonData,typeMappingPath);                   
                            Map<String, Object> locateId = gson.fromJson(eventData, typeToken.getType());
                            Map<String, Object> getOid = (Map<String, Object>) locateId.get("_id");   
                            ApiFuture<QuerySnapshot> query = collection.whereEqualTo("_id.$oid",getOid.get("$oid").toString()).get();
                            List<QueryDocumentSnapshot> documentId = query.get().getDocuments();
                            for (QueryDocumentSnapshot result : documentId) {
                            DocumentReference docRef = result.getReference();
                            docRef.update(event);
                                    break;}
                            break;
                    case "d"://for delete event 
                            ApiFuture<QuerySnapshot> getdocumentId = collection.whereEqualTo("_id.$oid",oid_str).get();
                            List<QueryDocumentSnapshot> result = getdocumentId.get().getDocuments();
                            for (QueryDocumentSnapshot doc : result) {
                                doc.getReference().delete();
                            }
                            break;
                    default:
                            System.out.println("OP is not Found");
                }
            } catch (JsonSyntaxException e) {
                System.err.print("Error occurred while deserializing JSON data: " + e.getMessage());
            } }catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (firestore != null) {
                firestore.close();
        }
    }
}

    private static String extractJsonString(String message, String key) {
        Pattern pattern = Pattern.compile( key + ": \"(.*?)\"(?=\\s|$)");
        Matcher matcher = pattern.matcher(message);
        if (matcher.find()) {
            return matcher.group(1).replace("\\\"", "\"");
        }
        return null;
    }
}