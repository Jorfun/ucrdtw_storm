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


public class Keogh2Bolt implements IRichBolt {
	 
  private OutputCollector collector;
  private Tools tools;
  private int size;	//size
  private double bsf;
  private int[] order;
  private double[] qo;

  private final double INF = 1e20;
  private final boolean TRACING = false;

  @Override
  public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {  
     
	  this.collector = collector;    
	  
	  this.tools = new Tools();
	  
	  String mStr = stormConf.get("size").toString();
	  
	  this.size = Integer.parseInt(mStr);
	  
	  this.bsf = INF;
	  
	  this.order = new int[this.size];
	  this.qo = new double[this.size];
	  
	  String orderStr = stormConf.get("order").toString();
	  String qoStr = stormConf.get("queryOrdered").toString();
	  
	  String[] strOrder = orderStr.split(" ");  
	  String[] strQo = qoStr.split(" "); 
	  
	  for(int i=0; i<this.size; i++)
	  {
		  this.order[i] = Integer.parseInt(strOrder[i]);
		  this.qo[i] = Double.parseDouble(strQo[i]);
		  //trace("("+i+")LB_Keogh1 target value is:" + target[i]);
	  }	 
	    
  }  
  
  public double lb_Keogh2_cumulative(String ld, String ud, double[] cb2, double mean, double std)
  {
	  // convert target string into values we can use
	  String[] strLd = ld.split(" ");  
	  double[] l = new double[this.size];
	  
	  for(int i=0; i<this.size; i++)
	  {
		  l[i] = Double.parseDouble(strLd[i]);
		  //trace("("+i+")LB_Keogh2 lower bound value is:" + l[i]);
	  }	  

	  String[] strUd = ud.split(" ");  
	  double[] u = new double[this.size];
	  
	  for(int i=0; i<this.size; i++)
	  {
		  u[i] = Double.parseDouble(strUd[i]);
		  //trace("("+i+")LB_Keogh2 upper bound value is:" + u[i]);
	  }	  	  
	  
	  
//	  for(int i=0; i<this.size; i++)
//	  {
//		  trace("("+i+")LB_Keogh2 order value is:" + this.order[i]);
//		  trace("("+i+")LB_Keogh2 qo value is:" + this.qo[i]);
//	  }		  		  
	  
	  //trace("m and bsf are:" + size + " " + bsf);
	  //trace("mean and std are:" + mean + " " + std);
	  
	  // start calculation
	  double lb = 0.0;
	  double uu, ll, d;
	  
	  for(int i=0; i<this.size && lb<this.bsf; i++)
	  {
		  uu = (u[this.order[i]] - mean) / std;
		  ll = (l[this.order[i]] - mean) / std;
		  d = 0.0;
		  if(this.qo[i] > uu)
			  d = tools.dist(this.qo[i], uu);
		  else if(qo[i] < ll)
			  d = tools.dist(this.qo[i], ll);
		  
		  lb += d;
		  cb2[this.order[i]] = d;
	  }
	  
	  return lb;
	  
  }
  
  
  @Override
  public void execute(Tuple input) {  
      
	  if(input.getSourceStreamId().equals("updateBsf"))
	  {
		  double newBsf = input.getDoubleByField("bsf");
		  
		  this.bsf = newBsf;
		  
		  trace("@@@@@@@@@@@@@@Keogh2Bolt update bsf value as: "+ this.bsf);
		  	  
	  }
	  else
	  {
		  
		  String target = input.getStringByField("target");
		  //String query = input.getStringByField("query");
		  String ld = input.getStringByField("lower bound");
		  String ud = input.getStringByField("upper bound");
		  String tempCb1 = input.getStringByField("cb1");
		  double mean = input.getDoubleByField("mean");
		  double std = input.getDoubleByField("std");
		  double keogh1 = input.getDoubleByField("keogh1");
		  int location = input.getIntegerByField("location");
		  
		  trace("@@@@@@@@@@ Keogh2Bolt bsf is: @@@@@@@@@@  " + this.bsf);	

		  String[] strCb1 = tempCb1.split(" ");  
		  double[] cb1 = new double[size];
		  
		  for(int i=0; i<size; i++)
		  {
			  cb1[i] = Double.parseDouble(strCb1[i]);
			  //trace("("+i+")LB_Keogh2 cb1 is:" + cb1[i]);
		  }		  
		  
		  
		  double[] cb2 = new double[this.size]; 
		  
		  for(int i=0; i<this.size; i++)
		  {
			  cb2[i] = 0.0;
		  }	  
		  
		  double result = lb_Keogh2_cumulative(ld, ud, cb2, mean, std);

		  
//		  for(int i=0; i<size; i++)
//		  {
//			  trace("("+i+")LB_Keogh2 cb2 is:" + cb2[i]);
//		  }		  
		  
		  trace("\n%%%%%%%%%%%  keogh1 is  %%%%%%%%%%%  "+keogh1);
		  trace("%%%%%%%%%%%  Keogh2 is  %%%%%%%%%%%  "+result+"\n");

		  
		  if(result < bsf)
		  {
			  double cb[] = new double[this.size];
			  
			  for(int i=0; i<this.size; i++)
			  {
				  cb[i] = 0.0;
			  }
			  
			  // choose better lower bound between lb_keogh and lb_keogh2 to be used in early abandoning DTW
			  // cb will be cumulative summed here for convenience of later usage
			  
			  if(keogh1 > result)
			  {
				  cb[this.size-1] = cb1[this.size-1];
				  for(int i=this.size-2; i>=0; i--)
					  cb[i] = cb[i+1] + cb1[i];
			  }
			  else
			  {
				  cb[this.size-1] = cb2[this.size-1];
				  for(int i=this.size-2; i>=0; i--)
					  cb[i] = cb[i+1] + cb2[i];
			  }
			  
			  
//			  for(int i=0; i<size; i++)
//			  {
//				  trace("("+i+")LB_Keogh2 cb is:" + cb[i]);
//			  }	
			  
			  
			  StringBuffer cbString = new StringBuffer();
			  
			  for(int i=0; i<this.size; i++)
			  {
				  if(i == 0)
				  {
					  cbString.append(String.valueOf(cb[i]));
					  continue;
				  }
				  
				  cbString.append(" " + String.valueOf(cb[i]));  
				  
			  }	
			  
			  
			  this.collector.emit(input, new Values(target, mean, std, location, cbString.toString())); 
		  }
		  	  	  
	  }
	  
 
	  this.collector.ack(input);
	  
  }  

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
	  
	  declarer.declare(new Fields("target", "mean", "std", "location", "cb"));
	  
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