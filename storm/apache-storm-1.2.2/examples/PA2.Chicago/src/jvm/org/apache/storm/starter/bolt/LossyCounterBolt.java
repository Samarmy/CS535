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

import java.util.Arrays;
import java.util.ArrayList;

public class LossyCounterBolt extends BaseRichBolt {
  private static final Logger LOG = Logger.getLogger(LossyCounterBolt.class);
  Map<String, Bin> counterMap;
  private OutputCollector collector;
  float epsilon = 1/100;
  int bCurrent = 1;
  int binCounter = 0;

  public LossyCounterBolt(float binSize){
    this.epsilon = 1/binSize;
  }

  @Override
  public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
    this.counterMap = new HashMap<String, Bin>();
    this.collector = collector;
  }

  @Override
  public void execute(Tuple tuple) {
    String key = tuple.getString(0);
    binCounter++;
    if(!counterMap.containsKey(key)){
       counterMap.put(key, new Bin(1, bCurrent - 1));
    }else{
       Bin c = counterMap.get(key);
       counterMap.put(key, new Bin(c.f + 1, c.delta));
    }
    Bin val = counterMap.get(key);
    if(binCounter == (1/epsilon)){
      binCounter = 0;
      ArrayList<String> remove = new ArrayList<String>();
      for(Map.Entry<String, Bin> entry:counterMap.entrySet()){
        Bin c = entry.getValue();
        if((c.f + c.delta) <= bCurrent){
          remove.add(entry.getKey());
        }
      }
      for(String r: remove){
        counterMap.remove(r);
      }
    bCurrent++;
    }
    int sum = val.f + val.delta;
    this.collector.emit(new Values(key, sum));

    collector.ack(tuple);
  }

  @Override
  public void cleanup() {}

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("hashtag", "count"));
  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    return null;
  }

  public class Bin{
    int f;
    int delta;

    public Bin(int f, int delta){
      this.f = f;
      this.delta = delta;
    };
  }

}
