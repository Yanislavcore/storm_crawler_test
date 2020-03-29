package org.yanislavcore;

import clojure.lang.PersistentVector;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class EsClientProvider {
    private static volatile RestHighLevelClient client;

    private EsClientProvider() {
    }

    /**
     * Creates ONLY one ES client per worker (JVM).
     *
     * @param conf - topology config
     */
    public static RestHighLevelClient initOrGetClient(Map conf) {
        if (client == null) {
            synchronized (PagesSpout.class) {
                if (client == null) {
                    client = buildClient(conf);
                }
            }
        }
        return client;
    }

    private static RestHighLevelClient buildClient(Map conf) {
        PersistentVector hosts = (PersistentVector) conf.get("es.hosts");
        long timeout = (long) conf.get("es.timeoutMillis");
        List<HttpHost> httpHosts = new ArrayList<>();
        //noinspection unchecked
        hosts.forEach(h -> httpHosts.add(HttpHost.create((String) h)));

        RestClientBuilder lowLevelClient = RestClient.builder(httpHosts.toArray(new HttpHost[]{}))
                .setRequestConfigCallback(b -> b.setConnectTimeout(Math.toIntExact(timeout)));
        return new RestHighLevelClient(lowLevelClient);
    }

}
