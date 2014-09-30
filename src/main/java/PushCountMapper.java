import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import com.google.gson.Gson;
import com.google.gson.JsonObject;

public class PushCountMapper  extends Mapper<Object, Text, Text, IntWritable> {

  @Override
  public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    JsonObject event = new Gson().fromJson(value.toString(), JsonObject.class);
    if(event.get("type") != null && !event.get("type").isJsonNull()
        && event.get("type").getAsString().equalsIgnoreCase("pushevent")
        && event.get("repository") != null && !event.get("repository").isJsonNull()
        && event.get("repository").getAsJsonObject().get("language") != null
        && !event.get("repository").getAsJsonObject().get("language").isJsonNull()) {
      String language = event.get("repository").getAsJsonObject().get("language").getAsString().toLowerCase();
      context.write(new Text(language), new IntWritable(1));
   }
  }

}
