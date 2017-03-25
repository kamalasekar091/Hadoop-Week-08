//package v4;
// cc MaxTemperatureMapperV4 Mapper for maximum temperature example
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class MalFormedMapper
  extends Mapper<LongWritable, Text, Text, IntWritable> {
	  


  private static final int MISSING = 9999;
  private final static IntWritable one = new IntWritable(1);
  
  
  enum Temperature {
    MALFORMED
  }

  //private NcdcRecordParser parser = new NcdcRecordParser();

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
		  
	String line = value.toString();
	String Station_ID = line.substring(4, 10);
    String Station_identifier= line.substring(10,15);
    String mapstation = Station_ID+"-"+Station_identifier;
    int airTemperature;
    if (line.charAt(87) == '+') { // parseInt doesn't like leading plus signs
      airTemperature = Integer.parseInt(line.substring(88, 92));
    } else {
      airTemperature = Integer.parseInt(line.substring(87, 92));
    }

    //parser.parse(value);
     if (airTemperature == MISSING) {
      context.write(new Text(mapstation), one);
    }
  }
}
