package storm.starter.bolt;


import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import storm.starter.utilities.Tools;


public class PreBolt implements IRichBolt {
	 
  private OutputCollector collector;
  private Tools tools;
  
  private int size;
  private int warp;		//warpingWindow
  //private StringBuffer query;
  
  private final boolean TRACING = false;
   
  @Override
  public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {  
     
	  this.collector = collector;   
	  this.tools = new Tools();
	//this.query = new StringBuffer();
      this.size = Integer.parseInt(stormConf.get("size").toString());
      this.warp = Integer.parseInt(stormConf.get("warpingWindow").toString());
      
  }
 

  @Override
  public void execute(Tuple input) {  
      
	  
	  String target = input.getStringByField("series");
	  double mean = input.getDoubleByField("mean");
	  double std = input.getDoubleByField("std");
	  int location = input.getIntegerByField("location");
	
	  //trace("PreBolt get value is:" + target);
	  
	  target = target.substring(1, target.length()-1);

	  // convert values we get into values we can use
	  String[] strTarget = target.split(", ");  
	  double[] t = new double[this.size];
	  
	  for(int i=0; i<this.size; i++)
	  {
		  t[i] = Double.parseDouble(strTarget[i]);
		  //trace("("+i+")PreBolt target value is:" + t[i]);
	  }	  	  
	  
	  double[] ld = new double[this.size];
	  double[] ud = new double[this.size];

	  //create envelope for target series
	  tools.lower_upper_lemire(t, ld, ud, this.size, this.warp);
	  
//	  trace("lower bound for data");
//	  for(int i=0; i<this.size; i++)
//	  {
//		  trace("("+i+") "+ld[i]);
//	  }
//	  trace("upper bound for data");
//	  for(int i=0; i<this.size; i++)
//	  {
//		  trace("("+i+") "+ud[i]);
//	  }
	  
	  // transform double arrays into strings that we can transit
	  StringBuffer ldString = new StringBuffer();
	  StringBuffer udString = new StringBuffer();
	  
	  for(int i=0; i<this.size; i++)
	  {
		  if(i == 0)
		  {
			  ldString.append(String.valueOf(ld[i]));
			  udString.append(String.valueOf(ud[i]));
			  continue;
		  }
		  
		  ldString.append(" " + String.valueOf(ld[i]));
		  udString.append(" " + String.valueOf(ud[i]));		  
		  
	  }
	     
//	  trace("~~~~~~~ PreBolt ldString.toString() ~~~~~~~ "+ldString.toString());
//	  trace("~~~~~~~ PreBolt udString.toString() ~~~~~~~ "+udString.toString());
	  
	  //this.collector.emit(input, new Values(target, this.query.toString(), mean, std, location));
	  this.collector.emit(input, new Values(target, ldString.toString(), udString.toString(), mean, std, location));
	  this.collector.ack(input);
	  
  }  

  
  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    //declarer.declare(new Fields("target", "query", "mean", "std", "location"));
	  declarer.declare(new Fields("target", "lower bound", "upper bound", "mean", "std", "location"));
  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public void cleanup() {
  	  
	  
  }
  
  public void trace(String s)
  {
	  if(TRACING)
	  {
		  System.out.println(s);
	  }
	    
  }
  
}

