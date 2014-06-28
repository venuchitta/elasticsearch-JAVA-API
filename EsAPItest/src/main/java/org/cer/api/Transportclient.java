package org.cer.api;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.cer.api.AbstractApi;


public class Transportclient extends AbstractApi {
    private static final String ID_NOT_FOUND = "<ID NOT FOUND>";


	public void connect() throws ElasticsearchException, IOException {
        final String indexName = "test";
        final String documentType = "tweet";
        final String documentId = "1";
        final String fieldName = "foo";
        final String value = "bar";
        final Integer term = 200;


		//For testing purposes we will create a test node so we can connect to
		createLocalCluster("escluster2");

		//Create the configuration - you can omit this step and use 
		//non-argument constructor of TransportClient
		Settings settings = ImmutableSettings.settingsBuilder()
				.put("cluster.name", "escluster2").build();

		//Create the transport client instance
		TransportClient client = new TransportClient(settings);

		//add some addresses of ElasticSearch cluster nodes
		client.addTransportAddress(new InetSocketTransportAddress("127.0.0.1", 9300));

		//Now we can do something with ElasticSearch
		//...
		final CreateIndexRequestBuilder createIndexRequestBuilder = client.admin().indices().prepareCreate(indexName);
        final XContentBuilder mappingBuilder = jsonBuilder().startObject().startObject(documentType)
                .startObject("_ttl").field("enabled", "true").field("default", "1s").endObject().endObject()
                .endObject();
        System.out.println(mappingBuilder.string());
        System.out.println(createIndexRequestBuilder.toString());
        createIndexRequestBuilder.addMapping(documentType, mappingBuilder).execute().actionGet();
        final IndexRequestBuilder indexRequestBuilder = client.prepareIndex(indexName, documentType, documentId);
        // build json object
        final XContentBuilder contentBuilder = jsonBuilder().startObject().prettyPrint();
        contentBuilder.field(fieldName, value,term);
        contentBuilder.field("boo","venu",123);
        indexRequestBuilder.setSource(contentBuilder).execute().actionGet();
        // Get document
        System.out.println(getValue(client, indexName, documentType, documentId, "boo"));

		//At the end we should close resources. In real scenario make sure do it in finally block.
		//client.close();
	}
    protected static String getValue(final Client client, final String indexName, final String documentType,
            final String documentId, final String fieldName) {
        final GetRequestBuilder getRequestBuilder = client.prepareGet(indexName, documentType, documentId);
        System.out.println(FilterBuilders.prefixFilter(indexName, "bo").cache(true).toString());
        getRequestBuilder.setFields(new String[] { fieldName });
        final GetResponse response2 = getRequestBuilder.execute().actionGet();
        

        
        
        if (response2.isExists()) {
            final String name = response2.getField(fieldName).getValue().toString();
            
            return name;
        } else {
            return ID_NOT_FOUND;
        }
    }
	

}
