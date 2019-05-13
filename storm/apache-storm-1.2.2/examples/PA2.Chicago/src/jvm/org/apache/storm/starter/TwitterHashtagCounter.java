package org.apache.storm.starter;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.starter.util.StormRunner;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.starter.spout.TwitterSpout;
import org.apache.storm.starter.bolt.HashtagReaderBolt;
import org.apache.storm.starter.bolt.HashtagCounterBolt;
import org.apache.log4j.Logger;

import java.util.*;

public class TwitterHashtagCounter {
  private static final Logger LOG = Logger.getLogger(TwitterHashtagCounter.class);
  private final TopologyBuilder builder;
  private final String topologyName;
  private final Config topologyConfig;

  public TwitterHashtagCounter(String topologyName) throws InterruptedException {
    builder = new TopologyBuilder();
    this.topologyName = topologyName;
    topologyConfig = createTopologyConfiguration();

    wireTopology();
  }

  private static Config createTopologyConfiguration() {
    Config conf = new Config();
    conf.setDebug(true);
    return conf;
  }

  private void wireTopology() throws InterruptedException {
    String spoutId = "twitter-spout";
    String readerId = "hashtag-reader-bolt";
    String counterId = "hashtag-counter-bolt";
    builder.setSpout(spoutId, new TwitterSpout(), 5);
    builder.setBolt(readerId, new HashtagReaderBolt(), 4).shuffleGrouping(spoutId);
    builder.setBolt(counterId, new HashtagCounterBolt(), 4).fieldsGrouping(readerId, new Fields("hashtag"));
  }

  public void runLocally() throws InterruptedException {
    LocalCluster localCluster = new LocalCluster();
    localCluster.submitTopology(topologyName, topologyConfig, builder.createTopology());
  }

  public void runRemotely() throws Exception {
    StormRunner.runTopologyRemotely(builder.createTopology(), topologyName, topologyConfig);
  }

  public static void main(String[] args) throws Exception {
    String topologyName = "twitterHashtagCounter";
      if (args.length >= 1) {
        topologyName = args[0];
      }
      boolean runLocally = true;
      if (args.length >= 2 && args[1].equalsIgnoreCase("remote")) {
        runLocally = false;
      }

      LOG.info("Topology name: " + topologyName);
      TwitterHashtagCounter thc = new TwitterHashtagCounter(topologyName);
      if (runLocally) {
        LOG.info("Running in local mode");
        thc.runLocally();
      }
      else {
        LOG.info("Running in remote (cluster) mode");
        thc.runRemotely();
      }
  }
}
