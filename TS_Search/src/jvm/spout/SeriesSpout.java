package storm.starter.spout;
 
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
 
//import storm.starter.hdfs.HdfsUtils;
 
public class SeriesSpout extends BaseRichSpout {
  		
  private ConcurrentHashMap<UUID, Values> pending;
  private SpoutOutputCollector collector;
 
  private FileReader dataReader;
  private BufferedReader dataBufferedReader;
  private ArrayList<Double> data;	// store data points of current subsequence
  private int size;		// query length
  private int readIndex, newValueIndex, seriesLoc; 
  private double ex, ex2;
  private boolean completed = false;
  
  private final int REMOVEINDEX = 0;
  
  private final boolean TRACING = true;
  
  @Override
  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
   
	this.collector = collector;
	this.pending = new ConcurrentHashMap<UUID, Values>();
	this.size = Integer.parseInt(conf.get("size").toString());
	this.data = new ArrayList<Double>();
	this.ex = 0.0;
	this.ex2 = 0.0;
	this.readIndex = 0;
	this.newValueIndex = 0;
	this.seriesLoc = 0;
	

    try {
    	
	        String dataFilePath = conf.get("dataFile").toString();  // 获取数据源文件路径
	        trace("data_file_path:" + dataFilePath);
	        
	        this.dataReader = new FileReader(dataFilePath);
	        this.dataBufferedReader = new BufferedReader(dataReader);
	        	                
         
    } catch (FileNotFoundException e) {
        e.printStackTrace();
    }  
    

  }

 
  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("series", "mean", "std", "location"));
  }  
  
  
  @Override
  public void nextTuple() {
	  
	//Utils.sleep(100);
		
	if(completed){  
		try {  
			Thread.sleep(1000);  
		} catch (InterruptedException e) {  
			//Do nothing  
		}  
		return;  
	}  	  
	
	String str;   
	
	while(data.size() < this.size - 1)
	{
		try {
				str = dataBufferedReader.readLine();
				
				trace("***********("+readIndex+") Read string is :***********" + str);
				
				if(str != null)
				{
					data.add(Double.parseDouble(str));
						
					double temp = data.get(newValueIndex);
						
					//trace("***********("+readIndex+") Retrieve number is :***********"+ temp);					 
					
					this.ex += temp;
					this.ex2 += temp * temp;	

					newValueIndex++;
					readIndex++;				    	
					
				}	    					

			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	
	if(data.size() >= this.size - 1)
	{
		
		
		try {
				str = dataBufferedReader.readLine();
				trace("***********("+readIndex+") Read string is :***********" + str);	    
				
				if(str != null)
				{
					data.add(Double.parseDouble(str));
					//trace("***********("+readIndex+") Retrieve number is :***********"+ temp);
					
					// calculate mean and std of this series
					double mean = 0.0, std = 0.0;
					double temp = data.get(newValueIndex);
					
					this.ex += temp;
					this.ex2 += temp * temp;
					
					mean = this.ex / this.size;
					std = this.ex2 / this.size;
					std = Math.sqrt(std - mean * mean);
					
					
					// emit essential data to next bolt
					str = data.toString();
										
					Values values = new Values(str, mean, std, seriesLoc);
					UUID msgId = UUID.randomUUID();
					this.pending.put(msgId, values);				
					this.collector.emit(values, msgId);  
					
					
					// remove first point in this series, as well as its cumulative values
					temp = data.get(REMOVEINDEX);
					this.ex -= temp;
					this.ex2 -= temp * temp;
					data.remove(REMOVEINDEX);
					
					readIndex++;	
					seriesLoc++;
				}
				else
				{
					completed = true;
				}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		
	}
	    
  }
 
  @Override
  public void ack(Object msgId) {
	  this.pending.remove(msgId);
  }
 
  @Override
  public void fail(Object msgId) {
	  this.collector.emit(this.pending.get(msgId), msgId);
  }
  
  public void trace(String s)
  {
	  if(TRACING)
	  {
		  System.out.println(s);
	  }
	    
  }
 
 
}