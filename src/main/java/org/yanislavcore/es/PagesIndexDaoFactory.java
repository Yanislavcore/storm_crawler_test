package org.yanislavcore.es;

import java.io.Serializable;
import java.util.Map;

/**
 * Inits or returns already inited Elastic Search Client.
 */
public interface PagesIndexDaoFactory extends Serializable {
    /**
     * Inits or returns already inited Elastic Search Client.
     *
     * @param conf - storm topology configuration
     * @return client
     */
    PagesIndexDao initOrGetClient(Map conf);
}
