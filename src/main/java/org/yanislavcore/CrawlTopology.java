package org.yanislavcore;

import com.digitalpebble.stormcrawler.ConfigurableTopology;
import com.digitalpebble.stormcrawler.Constants;
import com.digitalpebble.stormcrawler.bolt.*;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.yanislavcore.components.EsIndexer;
import org.yanislavcore.components.PagesSpout;
import org.yanislavcore.es.PagesIndexDaoFactory;
import org.yanislavcore.es.RealPagesIndexDaoFactory;
import org.yanislavcore.utils.RealTimeMachine;
import org.yanislavcore.utils.TimeMachine;

/**
 * Crawler topology class, builds all components and submits topology.
 */
public class CrawlTopology extends ConfigurableTopology {

    public static void main(String[] args) {
        ConfigurableTopology.start(new CrawlTopology(), args);
    }

    @Override
    protected int run(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        PagesIndexDaoFactory provider = new RealPagesIndexDaoFactory();
        TimeMachine realTimeMachine = new RealTimeMachine();

        builder.setSpout("spout", new PagesSpout(provider, realTimeMachine), 1);

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
        builder.setBolt("index", new EsIndexer(provider, realTimeMachine))
                .localOrShuffleGrouping("parse")
                .fieldsGrouping("fetch", Constants.StatusStreamName, furl)
                .fieldsGrouping("sitemap", Constants.StatusStreamName, furl)
                .fieldsGrouping("feeds", Constants.StatusStreamName, furl)
                .fieldsGrouping("parse", Constants.StatusStreamName, furl);

        return submit("crawl", conf, builder);
    }


}
