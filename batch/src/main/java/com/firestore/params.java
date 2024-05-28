package com.firestore;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;

    // Get values from user
    
    public interface params extends PipelineOptions, DataflowPipelineOptions {
        ValueProvider<String> getGcsPath();
        void setGcsPath(ValueProvider<String> gcsPath);

        ValueProvider<String> getprojectId();
        void setprojectId(ValueProvider<String> projectId );

        ValueProvider<String> getdatabase_name();
        void setdatabase_name(ValueProvider<String> database_name);

        ValueProvider<String> getcollection_name();
        void setcollection_name(ValueProvider<String> collection_name);

        ValueProvider<String> getbatch_Size();
        void setbatch_Size(ValueProvider<String> batch_Size);
        
}
