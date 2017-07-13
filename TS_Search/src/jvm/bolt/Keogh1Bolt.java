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


public class Keogh1Bolt implements IRichBolt {
	 
  private OutputCollector collector;
  private Tools tools;
  private int size;	//size
  private double bsf;
  private int[] order;
  private double[] lo;
  private double[] uo;

  private final double INF = 1e20;
  private final boolean TRACING = false;
  
  
  @Override
  public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {  
     
	  this.collector = collector;    
	  
	  this.tools = new Tools();
	  
	  String mStr = stormConf.get("size").toString();
	  
	  this.size = Integer.parseInt(mStr);
	  
	  this.bsf = INF;
	  
	  // get query related data from config and store in instance variable
	  this.order = new int[this.size];
	  this.lo = new double[this.size];
	  this.uo = new double[this.size];
	  
	  String orderStr = stormConf.get("order").toString();
	  String loStr = stormConf.get("lowerOrdered").toString();
	  String uoStr = stormConf.get("upperOrdered").toString();
	  
	  String[] strOrder = orderStr.split(" ");  
	  String[] strLo = loStr.split(" "); 
	  String[] strUo = uoStr.split(" "); 
	  
	  for(int i=0; i<this.size; i++)
	  {
		  this.order[i] = Integer.parseInt(strOrder[i]);
		  this.lo[i] = Double.parseDouble(strLo[i]);
		  this.uo[i] = Double.parseDouble(strUo[i]);
		  //trace("("+i+")LB_Keogh1 target value is:" + target[i]);
	  }	 
	    
  }  
  
  
  public double lb_Keogh1_cumulative(String t, double[] cb1, double mean, double std)
  {
	  // convert target string into values we can use
	  String[] strTarget = t.split(", ");  
	  double[] target = new double[this.size];
	  
	  for(int i=0; i<this.size; i++)
	  {
		  target[i] = Double.parseDouble(strTarget[i]);
		  trace("("+i+")LB_Keogh1 target value is:" + target[i]);
	  }	  
	  

	  
	  for(int i=0; i<this.size; i++)
	  {
		  trace("("+i+")LB_Keogh1 lo value is:" + lo[i]);
		  trace("("+i+")LB_Keogh1 uo value is:" + uo[i]);
		  trace("("+i+")LB_Keogh1 order value is:" + order[i]);
	  }	 	  
	  
	  
	  trace("m and bsf are:" + size + " " + bsf);
	  trace("mean and std are:" + mean + " " + std);
	  
	  // start calculation
	  double lb = 0;
	  double x, d;
	  
	  for(int i=0; i<this.size && lb<this.bsf; i++)
	  {
		  x = (target[this.order[i]] - mean) / std;
		  d = 0.0;
		  if(x > this.uo[i])
			  d = this.tools.dist(x, this.uo[i]);
		  else if(x < lo[i])
			  d = this.tools.dist(x, this.lo[i]);
		  
		  //trace("x: "+x+"   d: "+d);
		  lb += d;
		  cb1[this.order[i]] = d;
	  }
	  
	  return lb;
	  
  }
  
  
  @Override
  public void execute(Tuple input) {  
  
	  if(input.getSourceStreamId().equals("updateBsf"))
	  {
		  double newBsf = input.getDoubleByField("bsf");
		  
		  this.bsf = newBsf;
		  
		  trace("@@@@@@@@@@@@@@Keogh1Bolt update bsf value as: "+ this.bsf);
		  	  
	  }
	  else
	  {
		  String target = input.getStringByField("target");
		  String ld = input.getStringByField("lower bound");
		  String ud = input.getStringByField("upper bound");
		  double mean = input.getDoubleByField("mean");
		  double std = input.getDoubleByField("std");
		  double kim = input.getDoubleByField("kim");
		  int location = input.getIntegerByField("location");
		  
		  trace("@@@@@@@@@@ Keogh1Bolt bsf is: @@@@@@@@@@  " + this.bsf);	
		  
		  double[] cb1 = new double[this.size]; 
		  
		  for(int i=0; i<this.size; i++)
		  {
			  cb1[i] = 0.0;
		  }	  
		  
		  double result = lb_Keogh1_cumulative(target, cb1, mean, std);
		  trace("\n%%%%%%%%%%%  kim is  %%%%%%%%%%%  "+kim);
		  trace("%%%%%%%%%%%  Keogh1 is  %%%%%%%%%%%  "+result+"\n");

		  //trace("%%%%%%%%%%%  cb1.toString()  %%%%%%%%%%%  "+cb1String.toString());
		  
		  if(result < this.bsf)
		  {
			  
			  StringBuffer cb1String = new StringBuffer();
			  
			  for(int i=0; i<this.size; i++)
			  {
				  if(i == 0)
				  {
					  cb1String.append(String.valueOf(cb1[i]));
					  continue;
				  }
				  
				  cb1String.append(" " + String.valueOf(cb1[i]));  
				  
			  }	 
			  
			  this.collector.emit(input, new Values(target, ld, ud, mean, std, location, result, cb1String.toString())); 
			  
		  }
		  		  
		  
	  }
	  
	  this.collector.ack(input);
	  
  }  

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
	  
	  declarer.declare(new Fields("target", "lower bound", "upper bound", "mean", "std", "location", "keogh1", "cb1"));
	  
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