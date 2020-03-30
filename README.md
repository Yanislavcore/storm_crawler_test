# Storm Crawler Test

## Build

```shell script
mvn clean package
```

## Configuration 

Configuration is stored in `crawler-conf.yaml`. 
Most likely you will need to change only ES hosts and index name.

## Run local mode

Please note, storm 1.2.x is required!

``` sh
storm jar target/storm-crawler-test-0.1.jar org.yanislavcore.CrawlTopology -conf crawler-conf.yaml -local
```

## Notes

IMHO:

* StormCrawler project has weak functionality, awful documentation and poorly expandable or configurable. 
Also, it doesn't look like a well-supported project.
I wouldn't recommend it to use in real long-running projects.
* Apache Storm fits for these purposes well, but it's relatively low-level. 
I would carefully consider alternatives like Spark or Flink which provides both low-level and high-level functionality, 
more popular, have better support and large users and knowledge base.  
* ElasticSearch doesn't fit like crawling queue storage. It makes it more difficult to parallel queue between 
crawler nodes, it gives a huge chance that some scheduled urls will be crawled multiple times. 
Also, other queue solutions could give much better performance, which could cut-down ES costs. 