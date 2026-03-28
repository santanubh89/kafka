package com.kafka.consumer;

import com.google.gson.JsonParser;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.net.URI;

@Slf4j
public class ElasticSearchClient {

    public static RestHighLevelClient createESClient(String esConnectionUri) {
        RestHighLevelClient restHighLevelClient = null;
        URI connUri = URI.create(esConnectionUri);
        String userInfo = connUri.getUserInfo();
        if (userInfo == null) {
            restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), connUri.getScheme())));
        } else {
            String[] auth = userInfo.split(":");
            HttpHost httpHost = new HttpHost(connUri.getHost(), connUri.getPort(), connUri.getScheme());
            HttpHost[] httpHosts = new HttpHost[]{httpHost};
            BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));
            restHighLevelClient = new RestHighLevelClient(RestClient.builder(httpHosts)
                    .setHttpClientConfigCallback(httpAsyncClientBuilder -> httpAsyncClientBuilder
                            .setDefaultCredentialsProvider(credentialsProvider)
                            .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())));
        }
        return restHighLevelClient;
    }

    public static void createWikimediaESIndex(RestHighLevelClient restHighLevelClient, String esIndexName) throws IOException {
        boolean indexExists = restHighLevelClient.indices().exists(new GetIndexRequest(esIndexName), RequestOptions.DEFAULT);
        if (!indexExists) {
            CreateIndexRequest createIndexRequest = new CreateIndexRequest(esIndexName);
            restHighLevelClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
            log.info("Wikimedia index created.");
        } else {
            log.info("Wikimedia index already exists.");
        }
    }

    public static void indexDocument(RestHighLevelClient restHighLevelClient, String esIndexName, ConsumerRecord<String, String> record) {
        String id = extractId(record.value());
        IndexRequest indexRequest = new IndexRequest(esIndexName)
                .source(record.value(), XContentType.JSON)
                // .id(id == null ? record.topic() + "_" + record.partition() + "_" + record.offset() : id)
                .id(record.topic() + "_" + record.partition() + "_" + record.offset());
        try {
            IndexResponse response = restHighLevelClient.index(indexRequest, RequestOptions.DEFAULT);
            log.info("Document indexed with id: {}", response.getId());
        } catch (IOException e) {
            log.error("Error indexing document: {}", e.getMessage());
        } catch (Exception e) {
            log.error("Unexpected error indexing document: {}", e.getMessage());
        }
    }

    public static BulkResponse bulkRequest(RestHighLevelClient restHighLevelClient, BulkRequest bulkRequest) {
        BulkResponse bulkResponse = null;
        try {
            bulkResponse = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            log.error("IO error executing bulk request: {}", e.getMessage());
        } catch (Exception e) {
            log.error("Unexpected error executing bulk request: {}", e.getMessage());
        }
        return bulkResponse;
    }

    private static String extractId(String value) {
        return JsonParser.parseString(value).getAsJsonObject()
                .get("meta").getAsJsonObject()
                .get("id").getAsString();
    }
}
