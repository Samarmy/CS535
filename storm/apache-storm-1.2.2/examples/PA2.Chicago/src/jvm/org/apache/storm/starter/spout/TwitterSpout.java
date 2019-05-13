package org.apache.storm.starter.spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.apache.log4j.Logger;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import twitter4j.*;
import twitter4j.conf.*;

public class TwitterSpout extends BaseRichSpout {
  private static final Logger LOG = Logger.getLogger(TwitterSpout.class);

  SpoutOutputCollector _collector;
  TwitterStream _twitterStream;
  LinkedBlockingQueue<Status> queue = null;
  private static final String consumerKey = "ZVrRMyXUstvQGjdeZSjMvrhq0";
  private static final String consumerSecret = "JDXQ4r5S0ZbqhBQvKUBnH6EMLw5lNtBo9CPCjdR1GhjU1ZSvLT";
  private static final String accessToken = "1108447348210962432-pgAB79vs7jPoSubP6vxaZ8ErJWGW00";
  private static final String accessTokenSecret = "3D3PdlXpA9YV5YYRDDiHyFnuJuRrJAy8eNXHWV1Y3HtdO";


  @Override
  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
    queue = new LinkedBlockingQueue();
    _collector = collector;
    StatusListener listener = new StatusListener() {
            @Override
            public void onStatus(Status status) {
               queue.offer(status);
            }

            @Override
            public void onDeletionNotice(StatusDeletionNotice sdn) {}

            @Override
            public void onTrackLimitationNotice(int i) {}

            @Override
            public void onScrubGeo(long l, long l1) {}

            @Override
            public void onException(Exception ex) {}

            @Override
            public void onStallWarning(StallWarning arg0) {
               // TODO Auto-generated method stub
            }
         };

         ConfigurationBuilder cb = new ConfigurationBuilder();
         cb.setDebugEnabled(true)
           .setOAuthConsumerKey(consumerKey)
           .setOAuthConsumerSecret(consumerSecret)
           .setOAuthAccessToken(accessToken)
           .setOAuthAccessTokenSecret(accessTokenSecret);
         _twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
         _twitterStream.addListener(listener);
         _twitterStream.sample();
  }

  @Override
  public void nextTuple(){
    Object tw = queue.poll();
    if (tw == null) {
       Utils.sleep(50);
    } else {
       _collector.emit(new Values(tw));
    }
  }

  @Override
  public void close() {
    _twitterStream.shutdown();
  }

  @Override
  public void ack(Object id) {
  }

  @Override
  public void fail(Object id) {
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("tweet"));
  }
}
