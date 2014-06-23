package org.cer.api;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterIndexHealth;
import org.elasticsearch.action.admin.cluster.node.hotthreads.NodeHotThreads;
import org.elasticsearch.action.admin.cluster.node.hotthreads.NodesHotThreadsResponse;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.admin.cluster.node.info.PluginInfo;
import org.elasticsearch.action.admin.cluster.node.shutdown.NodesShutdownResponse;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsGroup;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.exists.types.TypesExistsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.common.collect.Maps;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.cer.api.AbstractApi;


public class Transportclient extends AbstractApi {
    private static final String ID_NOT_FOUND = "<ID NOT FOUND>";


	public void connect() throws ElasticsearchException, IOException {
        final String indexName = "test1";
        final String documentType = "tweet";
        final String documentId = "1";
        final String fieldName = "foo";
        final String value = "bar";


		//For testing purposes we will create a test node so we can connect to
		createLocalCluster("elasticsearch");

		//Create the configuration - you can omit this step and use 
		//non-argument constructor of TransportClient
		Settings settings = ImmutableSettings.settingsBuilder()
				.put("cluster.name", "elasticsearch").build();

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
        contentBuilder.field(fieldName, value);

        indexRequestBuilder.setSource(contentBuilder);
        indexRequestBuilder.execute().actionGet();
        // Get document
        System.out.println(getValue(client, indexName, documentType, documentId, fieldName));

		//At the end we should close resources. In real scenario make sure do it in finally block.
		client.close();
	}
    protected static String getValue(final Client client, final String indexName, final String documentType,
            final String documentId, final String fieldName) {
        final GetRequestBuilder getRequestBuilder = client.prepareGet(indexName, documentType, documentId);
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
