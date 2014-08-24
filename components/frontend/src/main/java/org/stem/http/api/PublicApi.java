package org.stem.http.api;

import org.glassfish.grizzly.http.server.HttpServer;

import java.io.IOException;

/**
 * Created by Lucas Amorim on 2014-08-23.
 */
public class PublicApi {

    private HttpServer server;

    public static void main(String args[]){

        try {
            PublicApi api = new PublicApi();
            api.start();
        } catch (Exception e) {
            System.err.println(e);
        }
    }

    public void start(){
        server = new HttpServer();
        try {
            server.start();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void stop(){
        if (server != null) {
            server.shutdownNow();
        }
    }
}
