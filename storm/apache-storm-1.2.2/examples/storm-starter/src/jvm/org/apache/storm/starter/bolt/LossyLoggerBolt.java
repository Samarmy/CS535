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

import java.util.Map;
import java.util.PriorityQueue;

import twitter4j.*;
import twitter4j.conf.*;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Iterator;

public class LossyLoggerBolt extends BaseRichBolt {
  private static final Logger LOG = Logger.getLogger(LossyLoggerBolt.class);
  PriorityQueue pq;
  private OutputCollector collector;
  long start = System.nanoTime();
  FileWriter fw;
  BufferedWriter bw;



  @Override
  public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
    this.pq = new PriorityQueue<Entry>();
    this.collector = collector;
    try{
      fw = new FileWriter("/s/chopin/k/grad/sarmst/cs535/storm/apache-storm-1.2.2/counterLog", true);
      bw = new BufferedWriter(fw);
    }catch(IOException e){
      e.printStackTrace();
    }

  }

  @Override
  public void execute(Tuple tuple) {
    String key = tuple.getStringByField("hashtag");
    int val = tuple.getIntegerByField("count");
    Iterator<Entry> iter1 = pq.iterator();

    Entry entr = null;
    ArrayList<Entry> rem = new ArrayList<Entry>();
    while(iter1.hasNext()){
      Entry e = iter1.next();
      if (e.s.equals(key)){
        rem.add(e);
      }
    }
    for(Entry ent: rem){
      pq.remove(ent);
    }

    pq.add(new Entry(key, val));
    if(pq.size() > 100){
      pq.poll();
    }


    long end = System.nanoTime();
    if(((end-start)/1000000000L) > 10L){
      String str = "";
      ArrayList<Entry> ary = new ArrayList<Entry>();
      Iterator<Entry> iter = pq.iterator();
      while(iter.hasNext()){
        ary.add(iter.next());
      }
      Collections.sort(ary);
      for(Entry entry: ary){
        str = "<" + entry.s + ">" + str;
      }
      str = "<" + Long.toString(start) + ">" + str + "\n";
      writeToFile(str);
      pq.clear();
      start = end;
    }
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

  public void writeToFile(String str){
  	try {
  		bw.write(str);
      bw.flush();
  		System.out.println("Done Writing");

  	} catch (IOException e) {

  		e.printStackTrace();

  	}
  }

  public class Entry implements Comparable<Entry>{
    String s;
    int n;
    public Entry(String s, int n){
      this.s = s;
      this.n = n;
    }

    @Override
    public int compareTo(Entry other){
      return this.n > other.n ? 1 : this.n < other.n ? -1 : 0;
    }
  }

}
