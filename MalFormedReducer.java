
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class MalFormedReducer
  extends Reducer<Text, IntWritable, Text, IntWritable> {

  private IntWritable result = new IntWritable();
  int max =0;
  Text maxWord = new Text();

  @Override
  public void reduce(Text key, Iterable<IntWritable> values,
      Context context)
      throws IOException, InterruptedException {

	  if(!key.toString().equals("999999-99999")){
        int sum = 0;
          for (IntWritable val : values) {
            sum += val.get();
          }
		  
		  if(sum > max)
            {
                max = sum;
                maxWord.set(key);
            }
	  }
		  
          //result.set(sum);
          //context.write(key, result);
  }
  
  @Override
  protected void cleanup(Context context) throws IOException, InterruptedException {
      context.write(maxWord, new IntWritable(max));
  }
  
}
