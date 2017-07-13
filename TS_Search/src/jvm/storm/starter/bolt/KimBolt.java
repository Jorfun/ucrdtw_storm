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


public class KimBolt implements IRichBolt {
	 
  private OutputCollector collector;
  private Tools tools;
  private int size;	//size
  private double bsf;
  private double[] query;

  private final double INF = 1e20;
  private final boolean TRACING = false;

  @Override
  public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {  
     
	  this.collector = collector;    
	  
	  this.tools = new Tools();
	  
	  String mStr = stormConf.get("size").toString();
	  
	  this.size = Integer.parseInt(mStr);
	  
	  this.bsf = INF;
	  
	  // get query from config and store in instance variable
	  this.query = new double[this.size];

	  String queryStr = stormConf.get("query").toString();
	  
	  String[] strQuery = queryStr.split(" ");  
	  
	  for(int i=0; i<this.size; i++)
	  {
		  this.query[i] = Double.parseDouble(strQuery[i]);
		  //trace("("+i+")LB_Keogh1 target value is:" + target[i]);
	  }	 
	  
  }  
  
  
  public double lb_kim_hierarchy(String t, double mean, double std)
  {
	  double d, lb;
	  
	  // convert values we get into values we can use
	  String[] strTarget = t.split(", ");  
	  double[] target = new double[this.size];
	  
	  for(int i=0; i<this.size; i++)
	  {
		  target[i] = Double.parseDouble(strTarget[i]);
		  trace("("+i+")LB_Kim target value is:" + target[i]);
	  }	  
	  
	  
	  for(int i=0; i<size; i++)
	  {
		  trace("("+i+")LB_Kim query value is:" + this.query[i]);
	  }	 
	      
	  
	  trace("m  bsf  are:" + size + " " + bsf);
	  trace("mean and std are:" + mean + " " + std);	  
	  
	  //start calculation of LB_Kim
	  //1 point at front and back
	  double x0 = (target[0] - mean) / std;
	  double y0 = (target[this.size-1] - mean) / std;
	  //double x0 = target[0], y0 = target[size-1];
	  lb = tools.dist(x0, this.query[0]) + tools.dist(y0, this.query[this.size-1]);
	  if(lb >= this.bsf)	return lb;
	  //trace("(1 point at front and back)  x0:"+x0+"  y0:"+y0+"  dist(x0, query[0])"+Tools.dist(x0, query[0])+"  dist(y0, query[size-1])"+Tools.dist(y0, query[size-1]));	
	  //trace("(1 point at front and back)  lb:" + lb);	
	  
	  //2 points at front
	  double x1 = (target[1] - mean) / std;
	  //double x1 = target[1];
	  d = tools.min(tools.dist(x1, this.query[0]), tools.dist(x0, this.query[1]));
	  d = tools.min(d, tools.dist(x1, this.query[1]));
	  lb += d;
	  if(lb >= this.bsf)	return lb;
	  //trace("(2 points at front)  d:" + d + "  lb:" + lb);
	  
	  //2 points at back
	  double y1 = (target[this.size-2] - mean) / std;
	  //double y1 = target[size-2];
	  d = tools.min(tools.dist(y1, this.query[this.size-1]), tools.dist(y0, this.query[this.size-2]));
	  d = tools.min(d, tools.dist(y1, this.query[this.size-2]));
	  lb += d;
	  if(lb >= this.bsf)	return lb;
	  //trace("(2 points at back)  d:" + d + "  lb:" + lb);
	  
	  //3 points at front
	  double x2 = (target[2] - mean) / std;
	  //double x2 = target[2];
	  d = tools.min(tools.dist(x0, this.query[2]), tools.dist(x1, this.query[2]));
	  d = tools.min(d, tools.dist(x2, this.query[2]));
	  d = tools.min(d, tools.dist(x2, this.query[1]));
	  d = tools.min(d, tools.dist(x2, this.query[0]));
	  lb += d;
	  if(lb >= this.bsf)	return lb;
	  //trace("(3 points at front)  d:" + d + "  lb:" + lb);
	  
	  //3 points at back
	  double y2 = (target[this.size-3] - mean) / std;
	  //double y2 = target[size-3];
	  d = tools.min(tools.dist(y0, this.query[this.size-3]), tools.dist(y1, this.query[this.size-3]));
	  d = tools.min(d, tools.dist(y2, this.query[this.size-3]));
	  d = tools.min(d, tools.dist(y2, this.query[this.size-2]));
	  d = tools.min(d, tools.dist(y2, this.query[this.size-1]));
	  lb += d;
	  //trace("(3 points at back)  d:" + d + "  lb:" + lb);
	  
	  return lb;
  }
  

   
  @Override
  public void execute(Tuple input) {  
      
	  //trace("~~~~~~~~~~~Let's see what exactly is : "+input.getSourceStreamId());
	  
	  if(input.getSourceStreamId().equals("updateBsf"))
	  {
		  double newBsf = input.getDoubleByField("bsf");
		  
		  this.bsf = newBsf;
		  
		  trace("@@@@@@@@@@@@@@KimBolt update bsf value as: "+ this.bsf);
		  	  
	  }
	  else
	  {
		  
		  String target = input.getStringByField("target");
		  String ld = input.getStringByField("lower bound");
		  String ud = input.getStringByField("upper bound");
		  double mean = input.getDoubleByField("mean");
		  double std = input.getDoubleByField("std");
		  int location = input.getIntegerByField("location");
		    
		  //trace("~~~~~~~ KimBolt ld ~~~~~~~ "+ld);
		  //trace("~~~~~~~ KimBolt ud ~~~~~~~ "+ud);
		  
		  trace("@@@@@@@@@@ KimBolt bsf is: @@@@@@@@@@  " + this.bsf);	
		  
		  double result = lb_kim_hierarchy(target, mean, std);
		  //trace("%%%%%%%%%%%Final LB_Kim is%%%%%%%%%%%  "+result);
		  
		  if(result < this.bsf)
		  {
			  this.collector.emit(input, new Values(target, ld, ud, mean, std, location, result)); 
		  }  
		  	  
		  
	  }
	  
	  
	  this.collector.ack(input);
	  
  }  

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
	  
	  declarer.declare(new Fields("target", "lower bound", "upper bound", "mean", "std", "location", "kim"));
	  
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
