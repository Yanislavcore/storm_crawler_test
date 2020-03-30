package org.yanislavcore.components;

import com.digitalpebble.stormcrawler.Metadata;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yanislavcore.es.PagesIndexDao;
import org.yanislavcore.es.PagesIndexDaoFactory;
import org.yanislavcore.es.data.PageUrlData;
import org.yanislavcore.utils.TimeMachine;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Pages spout.
 * It collects scheduled for crawling pages from ElasticSearch and emits them to downstream bolts.
 */
public class PagesSpout extends BaseRichSpout {

    private static final Logger LOG = LoggerFactory
            .getLogger(PagesSpout.class);
    private final ConcurrentLinkedQueue<PageUrlData> queue = new ConcurrentLinkedQueue<>();
    private final PagesIndexDaoFactory provider;
    private final TimeMachine timeMachine;
    private transient PagesIndexDao dao;
    private int shardID = -1;
    private int maxDocsPerShard;
    private String indexName;
    private long timeoutMillis;
    private volatile long lastRequestTimestamp = 0;
    private SpoutOutputCollector collector;

    public PagesSpout(PagesIndexDaoFactory provider, TimeMachine timeMachine) {
        this.provider = provider;
        this.timeMachine = timeMachine;
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        indexName = (String) conf.get("pagesIndex");
        maxDocsPerShard = Math.toIntExact((long) conf.get("pagesSpout.maxDocsPerRequestShard"));
        timeoutMillis = (long) conf.get("es.timeoutMillis");
        dao = provider.initOrGetClient(conf);
        initShardId(context);
    }

    private void initShardId(TopologyContext context) {
        int totalTasks = context.getComponentTasks(context.getThisComponentId()).size();
        if (totalTasks != 1) {
            shardID = context.getThisTaskIndex();
        } else {
            shardID = -1;
        }
    }

    private void tryRequestNextBatch() {
        if (timeMachine.epochMillis() - lastRequestTimestamp > timeoutMillis) {
            LOG.debug("Timeout is passed, requesting again!");
            requestNextBatch();
            lastRequestTimestamp = timeMachine.epochMillis();
        } else {
            LOG.debug("Request is in progress!");
        }
    }

    private void requestNextBatch() {
        dao.searchScheduledUrls(indexName, maxDocsPerShard, shardID, (res, ex) -> {
            lastRequestTimestamp = 0;
            if (ex != null) {
                LOG.error("Error, while requesting new data for spout", ex);
            } else {
                LOG.debug("Adding {} new urls to queue", res.size());
                queue.addAll(res);
            }
        });
    }

    @Override
    public void nextTuple() {
        PageUrlData next = queue.poll();
        if (next != null) {
            HashMap<String, String[]> meta = new HashMap<>();
            meta.put("idUrl", new String[]{next.id});
            LOG.debug("Emitting url {}", next.url);
            collector.emit(new Values(next.url, new Metadata(meta)));
        } else {
            tryRequestNextBatch();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("url", "metadata"));
    }

}
