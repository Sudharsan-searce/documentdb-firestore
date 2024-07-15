package com.google.cloud.documentdb.templates.batch;

import org.apache.beam.sdk.options.PipelineOptions;
    // Get values from user    
    public interface Utils extends PipelineOptions {
        String getGcsPath();
        void setGcsPath(String gcsPath);

        String getprojectId();
        void setprojectId(String projectId );

        String getdatabase_name();
        void setdatabase_name(String database_name);

        String getcollection_name();
        void setcollection_name(String collection_name);

        Integer getbatch_Size();
        void setbatch_Size(Integer batch_Size);     

        String getTypeMappingPath();
        void setTypeMappingPath(String typeMappingPath);
        
        String getFailTopic();
        void setFailTopic(String failTopic);
}