package storm.starter.bolt;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;


public class ReportBolt implements IRichBolt {
	 
  private OutputCollector collector;
  private FileWriter fileWriter;
  //private String outputFile;
  private double bsf;
  private int location;
  
  private final boolean TRACING = false;
  
  @Override
  public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {  
     
	  this.collector = collector;   
	  this.bsf = 1e20;
	  this.location = -1;
	  //this.outputFile = stormConf.get("outputFile").toString();
	  
	  try {
		  
	      this.fileWriter = new FileWriter(stormConf.get("outputFile").toString()); // 获取输出文件路径

	  } catch (IOException e) {
	      e.printStackTrace();
	  }
			 
	  
  }
   
  @Override
  public void execute(Tuple input) {  
      
	  double dtw = input.getDoubleByField("dtw");
	  int loc = input.getIntegerByField("location");
	
	  trace("ReportBolt get dtw is:" + dtw);
	  trace("ReportBolt get location is:" + loc);

//  	  try {	
//  		  fileWriter.write("~~~~~~~~~~ ReportBolt get dtw ~~~~~~~~~~ Distance:" + dtw + "  Location:" + loc + "\n"); // 写入输出文件
//  	  } catch (IOException e) {
//  	  	  e.printStackTrace();
//  	  }	  
	  
	  
	  if(dtw < this.bsf)
	  {
		  this.bsf = dtw;
		  this.location = loc;
		  
		  this.collector.emit("updateBsf", new Values(this.bsf));
		  
		  
		  try {
		      this.fileWriter.write("########## Update bsf ########## Distance:" + Math.sqrt(this.bsf) + "  Location:" + this.location + "\n"); // 写入输出文件
		      this.fileWriter.flush();	      
		  } catch (IOException e) {
		      e.printStackTrace();
		  }
		  
	  }
  
	  
	  this.collector.ack(input);
	  
  }  

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
	  
	  declarer.declareStream("updateBsf", new Fields("bsf"));
	  
  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public void cleanup() {
	  
	  trace("\n~~~~~~~~~~ Final result ~~~~~~~~~~ Distance:" + Math.sqrt(this.bsf) + "  Location:" + this.location + "\n");
  	  
  	  try {
  		  
          fileWriter.close();
          
      } catch (IOException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
      }
	  
  }
  
  public void trace(String s)
  {
	  if(TRACING)
	  {
		  System.out.println(s);
	  }
	    
  }
  
}

