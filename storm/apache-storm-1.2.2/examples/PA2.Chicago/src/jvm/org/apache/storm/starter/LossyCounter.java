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
import org.apache.storm.starter.bolt.LossyCounterBolt;
import org.apache.storm.starter.bolt.LossyLoggerBolt;
import org.apache.log4j.Logger;

import java.util.*;

public class LossyCounter {
  private static final Logger LOG = Logger.getLogger(LossyCounter.class);
  private final TopologyBuilder builder;
  private final String topologyName;
  private final Config topologyConfig;
  private static int binSize = 100;

  public LossyCounter(String topologyName) throws InterruptedException {
    builder = new TopologyBuilder();
    this.topologyName = topologyName;
    topologyConfig = createTopologyConfiguration();

    wireTopology();
  }

  private static Config createTopologyConfiguration() {
    Config conf = new Config();
    conf.setDebug(true);
    conf.setNumWorkers(10);
    return conf;
  }

  private void wireTopology() throws InterruptedException {
    String spoutId = "twitter-spout";
    String readerId = "hashtag-reader-bolt";
    String counterId = "lossy-counter-bolt";
    String loggerId = "lossy-logger-bolt";
    builder.setSpout(spoutId, new TwitterSpout(), 1);
    builder.setBolt(readerId, new HashtagReaderBolt(), 4).shuffleGrouping(spoutId);
    builder.setBolt(counterId, new LossyCounterBolt((float)binSize), 4).fieldsGrouping(readerId, new Fields("hashtag"));
    builder.setBolt(loggerId, new LossyLoggerBolt(), 1).globalGrouping(counterId);
  }

  public void runLocally() throws InterruptedException {
    LocalCluster localCluster = new LocalCluster();
    localCluster.submitTopology(topologyName, topologyConfig, builder.createTopology());
  }

  public void runRemotely() throws Exception {
    StormRunner.runTopologyRemotely(builder.createTopology(), topologyName, topologyConfig);
  }

  public static void main(String[] args) throws Exception {
    String topologyName = "lossyCounter";
      if (args.length >= 1) {
        topologyName = args[0];
      }
      boolean runLocally = true;
      if (args.length >= 2 && args[1].equalsIgnoreCase("remote")) {
        runLocally = false;
      }

      if (args.length >= 3) {
        binSize = Integer.parseInt(args[2].trim());
      }

      LOG.info("Topology name: " + topologyName);
      LossyCounter thc = new LossyCounter(topologyName);
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
