package org.apache.storm.starter.bolt;

import org.apache.storm.Config;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.TupleUtils;
import org.apache.log4j.Logger;
import org.apache.storm.starter.tools.NthLastModifiedTimeTracker;
import org.apache.storm.starter.tools.SlidingWindowCounter;

import java.util.HashMap;
import java.util.Map;

import twitter4j.*;
import twitter4j.conf.*;

public class HashtagCounterBolt extends BaseRichBolt {
  private static final Logger LOG = Logger.getLogger(HashtagCounterBolt.class);
  Map<String, Integer> counterMap;
  private OutputCollector collector;
  long start = System.nanoTime();

  @Override
  public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
    this.counterMap = new HashMap<String, Integer>();
    this.collector = collector;
  }

  @Override
  public void execute(Tuple tuple) {
    String key = tuple.getString(0);

    if(!counterMap.containsKey(key)){
       counterMap.put(key, 1);
    }else{
       Integer c = counterMap.get(key) + 1;
       counterMap.put(key, c);
    }

    collector.ack(tuple);
    long end = System.nanoTime();
    if((int)((end-start)/1000000000L) > 10){
      for(Map.Entry<String, Integer> entry:counterMap.entrySet()){
        System.out.println("Result: " + entry.getKey()+" : " + entry.getValue());
      }
      start = end;
    }
  }

  @Override
  public void cleanup() {
    for(Map.Entry<String, Integer> entry:counterMap.entrySet()){
      System.out.println("Result: " + entry.getKey()+" : " + entry.getValue());
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("hashtag"));
  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    return null;
  }
}
