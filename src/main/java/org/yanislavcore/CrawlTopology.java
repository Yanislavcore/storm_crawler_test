package org.yanislavcore;

import com.digitalpebble.stormcrawler.ConfigurableTopology;
import com.digitalpebble.stormcrawler.Constants;
import com.digitalpebble.stormcrawler.bolt.*;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.yanislavcore.components.EsIndexer;
import org.yanislavcore.components.PagesSpout;
import org.yanislavcore.es.EsClientProvider;
import org.yanislavcore.es.EsClientProviderImplementation;

/**
 * Crawler topology class, builds all components and submits topology.
 */
public class CrawlTopology extends ConfigurableTopology {

    public static void main(String[] args) throws Exception {
        ConfigurableTopology.start(new CrawlTopology(), args);
    }

    @Override
    protected int run(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        EsClientProvider provider = new EsClientProviderImplementation();

        builder.setSpout("spout", new PagesSpout(provider), 1);

        builder.setBolt("partitioner", new URLPartitionerBolt(), 10)
                .shuffleGrouping("spout");

        builder.setBolt("fetch", new FetcherBolt(), 10)
                .fieldsGrouping("partitioner", new Fields("key"));

        builder.setBolt("sitemap", new SiteMapParserBolt(), 10)
                .localOrShuffleGrouping("fetch");

        builder.setBolt("feeds", new FeedParserBolt(), 10)
                .localOrShuffleGrouping("sitemap");

        builder.setBolt("parse", new JSoupParserBolt(), 10)
                .localOrShuffleGrouping("feeds");

        Fields furl = new Fields("url");
        builder.setBolt("index", new EsIndexer(provider))
                .localOrShuffleGrouping("parse")
                .fieldsGrouping("fetch", Constants.StatusStreamName, furl)
                .fieldsGrouping("sitemap", Constants.StatusStreamName, furl)
                .fieldsGrouping("feeds", Constants.StatusStreamName, furl)
                .fieldsGrouping("parse", Constants.StatusStreamName, furl);

        return submit("crawl", conf, builder);
    }


}
