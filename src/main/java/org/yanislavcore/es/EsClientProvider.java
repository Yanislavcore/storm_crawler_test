package org.yanislavcore.es;

import org.elasticsearch.client.RestHighLevelClient;

import java.io.Serializable;
import java.util.Map;

/**
 * Inits or returns already inited Elastic Search Client.
 */
public interface EsClientProvider extends Serializable {
    /**
     * Inits or returns already inited Elastic Search Client.
     * @param conf - storm topology configuration
     * @return client
     */
    RestHighLevelClient initOrGetClient(Map conf);
}
