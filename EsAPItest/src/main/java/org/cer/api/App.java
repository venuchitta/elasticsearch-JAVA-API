// The JAVA REST API which stores the data from the connectors to the Elasticsearch.

package org.cer.api;

import java.io.IOException;

import org.elasticsearch.ElasticsearchException;


public class App 
{
    public static void main( String[] args ) throws ElasticsearchException, IOException
    {
       //TransportClientTest tc = new TransportClientTest();
       Transportclient tc = new Transportclient();
       tc.connect();
       System.out.println("Iam in main");
    }
}
