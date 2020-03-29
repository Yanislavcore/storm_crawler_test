package org.yanislavcore.es;

import org.elasticsearch.client.RestHighLevelClient;

import java.io.Serializable;
import java.util.Map;

public interface EsClientProvider extends Serializable {
    RestHighLevelClient initOrGetClient(Map conf);
}
