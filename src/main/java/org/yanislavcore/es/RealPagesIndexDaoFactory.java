package org.yanislavcore.es;

import clojure.lang.PersistentVector;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.yanislavcore.components.PagesSpout;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


/**
 * Inits or returns already inited real Elastic Search client.
 * Creates only one ES client per worker (JVM).
 */
public class RealPagesIndexDaoFactory implements PagesIndexDaoFactory {
    //Creates ONLY one ES client per worker (JVM).
    private static volatile PagesIndexDao dao;

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

    /**
     * Creates ONLY one ES client per worker (JVM).
     *
     * @param conf - storm topology configuration
     * @return client
     */
    @Override
    public PagesIndexDao initOrGetClient(Map conf) {
        if (dao == null) {
            synchronized (PagesSpout.class) {
                if (dao == null) {
                    RestHighLevelClient restClient = buildClient(conf);
                    dao = new PagesIndexDao(restClient);
                }
            }
        }
        return dao;
    }

}
