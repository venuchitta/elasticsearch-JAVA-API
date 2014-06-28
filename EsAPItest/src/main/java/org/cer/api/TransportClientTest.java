package org.cer.api;

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
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.common.collect.Maps;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.cer.api.AbstractApi;


public class TransportClientTest extends AbstractApi {

	public void connect() throws ElasticsearchException, IOException {
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
		

		// Using JSON to create an index and type
		String json = "{" +
				"\"user\":\"kimchy\"," +
				"\"postDate\":\"2013-01-30\"," +
				"\"message\":\"trying out Elasticsearch\"" +
				"}";
		IndexResponse response = client.prepareIndex("twitter", "tweet")
				.setSource(json)
				.execute()
				.actionGet();
		QueryBuilder qb = QueryBuilders.termQuery("user", "kimchy");
		System.out.println(qb.toString());
		System.out.println("Venus system runnig");
		System.out.println(response.getIndex());
		

		// Getting the health of the cluster
		ClusterHealthResponse resp = client.admin().cluster()
				.prepareHealth("twitter")
				.execute().actionGet();

		for (Entry<String, ClusterIndexHealth> entry : resp.getIndices().entrySet()) {
			System.out.println("Index: " + entry.getKey() + " (status: " + entry.getValue().getStatus() + ")");
			System.out.println("This index has : " 
					+ entry.getValue().getNumberOfShards() + " shards"
					+ " and "
					+ entry.getValue().getNumberOfReplicas() + " replicas.");
		}

		// Hot Threads API returns information about which part of ElasticSearch code 
		// are hot spots from the CPU side or where ElasticSearch is stuck for some reason.
		// curl localhost:9200/_nodes/hot_threads
		NodesHotThreadsResponse response1 = client.admin().cluster()
				.prepareNodesHotThreads()
				.execute().actionGet();

		for (NodeHotThreads nodeHotThreads : response1.getNodes()) {
			System.out.println(nodeHotThreads.getHotThreads());
		}

		// Getting the info about the nodes
		NodesInfoResponse nodesResponse = client.admin().cluster()
				.prepareNodesInfo()
				.setNetwork(true)
				.setPlugins(true)
				.execute().actionGet();

		for( NodeInfo node : nodesResponse.getNodes()) {
			System.out.println("Node eth address: " + node.getNetwork().primaryInterface().getMacAddress());
			System.out.println("Plugins: ");
			for( PluginInfo plugin : node.getPlugins().getInfos()) {
				System.out.println(plugin.getName() + " " + plugin.getUrl());
			}
		}

		// Response during the shut down
		NodesShutdownResponse NSDresponse = client.admin().cluster()
				.prepareNodesShutdown()
				.execute().actionGet();

		for (DiscoveryNode node : NSDresponse.getNodes()) {
			System.out.println(node.getName());
		}

		// Docs that are present in the cluster
		NodesStatsResponse NSRresponse = client.admin().cluster()
				.prepareNodesStats()
				.all()
				.execute().actionGet();


		for( NodeStats node : NSRresponse.getNodes()) {
			System.out.println("Doc count: " + node.getIndices().getDocs().getCount());
		}

		// Get shards response
		ClusterSearchShardsResponse GSresponse = client.admin().cluster()
				.prepareSearchShards()
				.setIndices("twitter")
				.setRouting("12")
				.execute().actionGet();

		for (ClusterSearchShardsGroup shardGroup : GSresponse.getGroups()) {
			System.out.println(shardGroup.getIndex() + "/" + shardGroup.getShardId());
		}


		ClusterStateResponse CSRresponse = client.admin().cluster()
				.prepareState()
				.execute().actionGet();

		String rn = CSRresponse.getState().getRoutingNodes().prettyPrint();
		System.out.println(rn);

		for (Entry<String, IndexRoutingTable> entry : CSRresponse.getState().getRoutingTable().indicesRouting().entrySet()) {
			System.out.println("Index: " + entry.getKey());
			System.out.println(entry.getValue().prettyPrint());
		}

		
		Map<String, Object> map = Maps.newHashMap();
		map.put("indices.ttl.interval", "10m");
		
		ClusterUpdateSettingsResponse CUSRresponse = client.admin().cluster()
			.prepareUpdateSettings()
				.setTransientSettings(map)
			.execute().actionGet();
		
		System.out.println(CUSRresponse.getTransientSettings().toDelimitedString(','));


		//At the end we should close resources. In real scenario make sure do it in finally block.
		client.close();
	}

}
