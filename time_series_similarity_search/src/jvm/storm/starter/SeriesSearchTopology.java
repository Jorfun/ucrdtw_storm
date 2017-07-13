/**
 * 
 * Find the most similar subsequence to given query series from a long time series.
 * 
 * Preprocess to the query time series (z-normaliztion, get sorting order, create envelope).
 * Create our storm topology and eploy it to the Storm cluster.
 * 
 */

package storm.starter;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;

import storm.starter.bolt.DTWBolt;
import storm.starter.bolt.Keogh1Bolt;
import storm.starter.bolt.Keogh2Bolt;
import storm.starter.bolt.KimBolt;
import storm.starter.bolt.PreBolt;
import storm.starter.bolt.ReportBolt;
import storm.starter.spout.SeriesSpout;
import storm.starter.utilities.Index;
import storm.starter.utilities.Tools;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;

public class SeriesSearchTopology {

  // define spout and bolt names
  private static final String SPOUT_ID = "spout";
  private static final String PRE_BOLT_ID = "pre-bolt";
  private static final String KIM_BOLT_ID = "Kim-bolt";
  private static final String KEOGH1_BOLT_ID = "Keogh1-bolt";
  private static final String KEOGH2_BOLT_ID = "Keogh2-bolt";
  private static final String DTW_BOLT_ID = "dtw-bolt";
  private static final String REPORT_BOLT_ID = "report-bolt";

  // process query file and save relevant data into config object
  public static void processQuery(Config conf, String queryFilePath, int size, int warp) {

    FileReader queryReader;
    BufferedReader queryBufferedReader;

    int i = 0;
    double d = 0.0, ex = 0.0, ex2 = 0.0, mean = 0.0, std = 0.0;
    double[] q = new double[size];
    double[] l = new double[size];
    double[] u = new double[size];
    double[] qo = new double[size];
    double[] uo = new double[size];
    double[] lo = new double[size];
    int[] order = new int[size];
    Index[] Q_tmp = new Index[size];
    Tools tools = new Tools();

    for (int count = 0; count < size; count++) {
      Q_tmp[count] = new Index();
    }

    String str = "";

    try {

      queryReader = new FileReader(queryFilePath);
      queryBufferedReader = new BufferedReader(queryReader);

      // Read query file and do z-normalization to the query
      while ((str = queryBufferedReader.readLine()) != null) {
        d = Double.parseDouble(str);

        ex += d;
        ex2 += d * d;
        q[i] = d;
        i++;

        //trace("*********("+i+"Show previous query*********" + str);
      }

    } catch (FileNotFoundException e1) {
      // TODO Auto-generated catch block
      e1.printStackTrace();
    } catch (IOException e2) {
      // TODO Auto-generated catch block
      e2.printStackTrace();
    }

    // do z-normaliztion to the query
    mean = ex / size;
    std = ex2 / size;
    std = Math.sqrt(std - mean * mean);

    for (int count = 0; count < size; count++) {
      q[count] = (q[count] - mean) / std;
      //trace("*********("+count+") Show after query*********" + q[count]);

      // Q_tmp for later sorting of query array
      Q_tmp[count].setValue(q[count]);
      Q_tmp[count].setIndex(count);
    }

    // create envelope of the query: lower bound l and upper bound u
    tools.lower_upper_lemire(q, l, u, size, warp);

    // sort the query one time by abs(z-norm(q[i]))
    Arrays.sort(Q_tmp);

    // for(int count=0; count<size; count++)
    // {
    //    trace("*********("+count+") Q_tmp*********" + Q_tmp[count].getIndex() + "   " + Q_tmp[count].getValue());
    // }

    // also create another arrays for keeping sorted envelope
    for (int count = 0; count < size; count++) {
      int or = Q_tmp[count].getIndex();
      order[count] = or;
      qo[count] = q[or];
      uo[count] = u[or];
      lo[count] = l[or];

    }

    // store these arrays in config for later usage
    StringBuffer qString = new StringBuffer();
    StringBuffer orderString = new StringBuffer();
    StringBuffer qoString = new StringBuffer();
    StringBuffer loString = new StringBuffer();
    StringBuffer uoString = new StringBuffer();

    for (int count = 0; count < size; count++) {
      if (count == 0) {
        qString.append(String.valueOf(q[count]));
        orderString.append(String.valueOf(order[count]));
        qoString.append(String.valueOf(qo[count]));
        loString.append(String.valueOf(lo[count]));
        uoString.append(String.valueOf(uo[count]));
        continue;
      }

      qString.append(" " + String.valueOf(q[count]));
      orderString.append(" " + String.valueOf(order[count]));
      qoString.append(" " + String.valueOf(qo[count]));
      loString.append(" " + String.valueOf(lo[count]));
      uoString.append(" " + String.valueOf(uo[count]));

    }

    conf.put("query", qString.toString());
    conf.put("order", orderString.toString());
    conf.put("queryOrdered", qoString.toString());
    conf.put("lowerOrdered", loString.toString());
    conf.put("upperOrdered", uoString.toString());
    //trace("\n*********Process query Yeah!!!!!!!*********\n");

  }

  public static void main(String[] args) throws Exception {

    SeriesSpout spout = new SeriesSpout();
    PreBolt preBolt = new PreBolt();
    KimBolt kimBolt = new KimBolt();
    Keogh1Bolt keogh1Bolt = new Keogh1Bolt();
    Keogh2Bolt keogh2Bolt = new Keogh2Bolt();
    DTWBolt dtwBolt = new DTWBolt();
    ReportBolt reportBolt = new ReportBolt();

    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout(SPOUT_ID, spout, 1);
    builder.setBolt(PRE_BOLT_ID, preBolt, 2).shuffleGrouping(SPOUT_ID);
    builder.setBolt(KIM_BOLT_ID, kimBolt, 2).shuffleGrouping(PRE_BOLT_ID).allGrouping(REPORT_BOLT_ID, "updateBsf");
    builder.setBolt(KEOGH1_BOLT_ID, keogh1Bolt, 2).shuffleGrouping(KIM_BOLT_ID).allGrouping(REPORT_BOLT_ID,
        "updateBsf");
    builder.setBolt(KEOGH2_BOLT_ID, keogh2Bolt, 2).shuffleGrouping(KEOGH1_BOLT_ID).allGrouping(REPORT_BOLT_ID,
        "updateBsf");
    builder.setBolt(DTW_BOLT_ID, dtwBolt, 2).shuffleGrouping(KEOGH2_BOLT_ID).allGrouping(REPORT_BOLT_ID, "updateBsf");
    builder.setBolt(REPORT_BOLT_ID, reportBolt, 1).shuffleGrouping(DTW_BOLT_ID);

    for (int i = 0; i < args.length; ++i) {
      System.out.println("arg " + i + ":" + args[i]);
    }

    String topName = args[0];
    String runMode = args[1];
    String dataFilePath = args[2];
    String queryFilePath = args[3];
    String outputFilePath = args[4];
    String m = args[5];
    String RStr = args[6];

    double R = Double.parseDouble(RStr);
    int r;

    if (R <= 1) {
      r = (int) Math.floor(R * Integer.parseInt(m));
    } else {
      r = (int) Math.floor(R);
    }

    Config conf = new Config();
    conf.put("dataFile", dataFilePath);
    conf.put("outputFile", outputFilePath);
    conf.put("size", m);
    conf.put("warpingWindow", r);

    //System.out.println("Topology warpingWindow: "+r);

    processQuery(conf, queryFilePath, Integer.parseInt(m), r);

    conf.setDebug(true);

    //System.out.println("Before Start...");

    //if (args != null && args.length > 0) {
    if (runMode.equalsIgnoreCase("srv")) { // 集群运行模式

      System.out.println("Server Mode...");

      conf.setNumWorkers(3);

      StormSubmitter.submitTopology(args[0], conf, builder.createTopology());

    } else if (runMode.equalsIgnoreCase("loc")) { // 单机运行模式

      System.out.println("Local Mode...");

      conf.setMaxTaskParallelism(3);

      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("UCR_DTW", conf, builder.createTopology());

      Thread.sleep(30000);
      cluster.killTopology("UCR_DTW");
      cluster.shutdown();

    } else {

      System.out.println("Unknown Mode:" + runMode);

    }

  }

}