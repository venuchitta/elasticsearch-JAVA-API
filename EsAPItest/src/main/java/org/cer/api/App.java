package org.cer.api;


/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
       TransportClientTest tc = new TransportClientTest();
       tc.connect();
       System.out.println("Iam in main");
    }
}
