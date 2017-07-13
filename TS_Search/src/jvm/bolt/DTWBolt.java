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


public class DTWBolt implements IRichBolt {
	 
  private OutputCollector collector;
  private Tools tools;
  private int size;	//size
  private int warp;	//warping window
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
	  
	  String rStr = stormConf.get("warpingWindow").toString();
	  
	  this.warp = Integer.parseInt(rStr);
	  
	  this.bsf = INF;
	  
	  this.query = new double[this.size];
	  
	  String queryStr = stormConf.get("query").toString();
	  
	  String[] strQuery = queryStr.split(" ");  
	  
	  for(int i=0; i<this.size; i++)
	  {
		  this.query[i] = Double.parseDouble(strQuery[i]);
		  //trace("("+i+")LB_Keogh1 target value is:" + target[i]);
	  }	 
	  
  }
   
  
  // calculate dtw distance between target series and query
  public double dtw(String t, String tempCb, double mean, double std, double bsf)
  {
	  
	  int i, j, k;
	  double x, y, z, min_cost;
	  double[] cost = new double[this.warp * 2 + 1];
	  double[] cost_prev = new double[this.warp * 2 + 1];
	  double[] cost_tmp;
	  
	  // initialize double arrays
	  
	  for(k=0; k<2*this.warp+1; k++)
	  {
		  cost[k] = INF;
		  cost_prev[k] = INF;
	  }
	   
	  // convert values we get into values we can use
	  String[] strTarget = t.split(", ");  
	  double[] target = new double[this.size];
	  	  
	  for(i=0; i<this.size; i++)
	  {
		  target[i] = Double.parseDouble(strTarget[i]);
		  //trace("("+i+")dtw target value is:" + target[i]);
	  }	  

	  // convert values we get into values we can use
	  String[] strCb = tempCb.split(" ");  
	  double[] cb = new double[this.size];
	  	  
	  for(i=0; i<this.size; i++)
	  {
		  cb[i] = Double.parseDouble(strCb[i]);
		  //trace("("+i+")dtw cb value is:" + cb[i]);
	  }		  
	  
	  
	  for(i=0; i<this.size; i++)
	  {
		  //trace("("+i+")dtw query value is:" + query[i]);
	  }
	  
	  
	  //trace("m   r   bsf  are:" + size + " " + warp + " " + bsf);
	  //trace("mean and std are:" + mean + " " + std);

	  
	  double temp;
	  
	  // start calculation of DTW
	  for(i=0; i<this.size; i++)
	  {
		  k = tools.max(0, this.warp-i);
		  min_cost = INF;
		  
		  temp = (target[i] - mean) / std;
		  
		  for(j=tools.max(0,i-this.warp); j<=tools.min(this.size-1,i+this.warp); j++, k++)
		  {
			  // initialize all row and column
			  if((i==0) && (j==0))
			  {
				  cost[k] = tools.dist(this.query[0], temp);
				  min_cost = cost[k];
				  continue;		  
			  }
			  
			  if((j-1 < 0) || (k-1 < 0))	y = INF;
			  else							y = cost[k-1];
			  if((i-1 < 0) || (k+1 > 2*this.warp))		x = INF;
			  else									  	x = cost_prev[k+1];
			  if((i-1 < 0) || (j-1<0))		z = INF;
			  else							z = cost_prev[k];
			  
			  
			  cost[k] = tools.min(tools.min(x,y), z) + tools.dist(temp, this.query[j]);
			  
			  if(cost[k] < min_cost)
			  {
				  min_cost = cost[k];
			  }
			  
		  }
		  
		  // We can abandon early if the current cummulative distance with lower bound together are larger than bsf
		  if(i+this.warp < this.size-1  &&  min_cost + cb[i+this.warp+1] >= bsf)
		  {
			  trace("----------Early abandoning of DTW----------" + (min_cost + cb[i+this.warp+1]));
			  return min_cost + cb[i+this.warp+1];
		  }
		  
		  
		  //Move current array to previous array
		  cost_tmp = cost;
		  cost = cost_prev;
		  cost_prev = cost_tmp;
		  
	  }
	  
	  k--;
	  
	  double final_dtw = cost_prev[k];	  
	  
	  return final_dtw;
  }
  
  
  @Override
  public void execute(Tuple input) {  
  
	  if(input.getSourceStreamId().equals("updateBsf"))
	  {
		  double newBsf = input.getDoubleByField("bsf");
		  
		  this.bsf = newBsf;
		  
		  trace("@@@@@@@@@@@@@@DTWBolt update bsf value as: "+ this.bsf);
		  	  
	  }
	  else
	  {
		  
		  String target = input.getStringByField("target");
		  String cb = input.getStringByField("cb");
		  double mean = input.getDoubleByField("mean");
		  double std = input.getDoubleByField("std");
		  int location = input.getIntegerByField("location");	 
		  trace("@@@@@@@@@@ DTWBolt bsf is: @@@@@@@@@@  " + this.bsf);
		  
//		  trace("@@@@@@@@@@ DTWBolt get target is: @@@@@@@@@@  " + target);
//		  trace("@@@@@@@@@@ DTWBolt get cb is: @@@@@@@@@@  " + cb);
//		  trace("@@@@@@@@@@ DTWBolt get mean is: @@@@@@@@@@  " + mean);
//		  trace("@@@@@@@@@@ DTWBolt get std is: @@@@@@@@@@  " + std);
//		  trace("@@@@@@@@@@ DTWBolt get location is: @@@@@@@@@@  " + location);
		  
		  
		  double result = dtw(target, cb, mean, std, this.bsf);
		  trace("%%%%%%%%%%%  dtw is  %%%%%%%%%%%  "+result);
		  
		  this.collector.emit(input, new Values(result, location));  
		  
	  }
	  

	  this.collector.ack(input);
	  
  }  

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
	  declarer.declare(new Fields("dtw", "location"));
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

