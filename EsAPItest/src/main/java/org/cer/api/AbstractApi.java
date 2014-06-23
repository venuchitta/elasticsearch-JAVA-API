package org.cer.api;

import java.io.IOException;
import java.util.logging.Logger;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

public class AbstractApi {
	protected static final Logger LOG = Logger.getLogger("TEST");
	
	private Client client;
	private Node node;

	
	/**
	 * Returns ElasticSearch client.
	 * @return client
	 */
	public Client getClient() {
		if (this.client != null) {
			return this.client;
		}
		
		NodeBuilder builder = NodeBuilder.nodeBuilder();
		
		Settings settings = ImmutableSettings.settingsBuilder()
				.put("gateway.type", "none")
				.build();
		
		builder.settings(settings).local(true).data(true);

	     this.node = builder.node();
	     this.client = node.client();
		
        return this.client;
	}
	
	public void createLocalCluster(final String clusterName) {
		NodeBuilder builder = NodeBuilder.nodeBuilder();
		
		Settings settings = ImmutableSettings.settingsBuilder()
				.put("gateway.type", "none")
				.put("cluster.name", "escluster2")
				.build();
		
		builder.settings(settings).local(false).data(true);

	     this.node = builder.node();
	     this.client = node.client();		
	}
	
	/**
	 * Create index with given name if index doesn't exist.
	 * @param index index name
	 */
	public void createIndexIfNeeded(final String index) {
		if (!existsIndex(index)) {
			getClient().admin().indices().prepareCreate(index).execute().actionGet();
		}
	}
	
	public void recreateIndex(final String index) {
		logger("Recreting index: " + index);
		if (existsIndex(index)) {
			getClient().admin().indices().prepareDelete(index).execute().actionGet();
		}
		getClient().admin().indices().prepareCreate(index).execute().actionGet();
	}
	
	public boolean existsIndex(final String index) {
		IndicesExistsResponse response = getClient().admin().indices().prepareExists(index).execute().actionGet();
		return response.isExists();		
	}
	
	protected void logger(final String msg) {
		LOG.info("* " + msg);
	}
	
	public void close() {
		if(client != null) {
			client.close();
		}
		
		if (node != null) {
			node.close();
		}
	}
	
	protected void createSomeBooks(final String index) throws ElasticsearchException, IOException {
		getClient().prepareBulk()
			.add(new IndexRequestBuilder(getClient(), "library").setId("1").setType("book").setSource(
				XContentFactory.jsonBuilder()
					.startObject()
						.field("title", "Book A Title")
						.field("num", 1)
					.endObject()
					).request())
			.add(new IndexRequestBuilder(getClient(), "library").setType("book").setSource(
				XContentFactory.jsonBuilder()
					.startObject()
						.field("title", "Book B Title")
						.field("num", 10)
					.endObject()
					).request())
			.execute().actionGet();
	}
	
}
