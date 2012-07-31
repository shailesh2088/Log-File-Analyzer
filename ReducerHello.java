package hello.world;

import java.io.*;
import java.util.*;
import java.util.Map.Entry;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;



public class ReducerHello extends Reducer<Text, Text, Text, IntWritable> {
	private HashMap<String, Integer> dtCount = new HashMap<String,Integer>();
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException{
		org.apache.hadoop.conf.Configuration conf = context.getConfiguration();
		String productId = conf.get("ProductId");
		String countryName = conf.get("CountryName");
		String searchDate = conf.get("SearchDate");
		int sum=0;
		String dt = "";
		Text key1 = new Text();
		IntWritable count = new IntWritable();
		for (Text val : values){
			if (productId != null){
				String str1 = val.toString();
				if(str1.indexOf("-")>0){
					if(dtCount.containsKey(str1))
						dtCount.put(str1, new Integer(dtCount.get(str1).toString())+1);
					else
						dtCount.put(str1, 1);
				}
				else{
					String str = val.toString();
					sum += new Integer(str);
				}	
			}
			else{
				String str = val.toString();
				sum += new Integer(str);
			}
		}
		if(productId != null && dtCount.size() != 0){
			Collection cs = dtCount.values();
			
			int maxCnt = new Integer(Collections.max(cs).toString());
			for(Entry<String ,Integer> entry : dtCount.entrySet()){
				if(entry.getValue() == maxCnt)
					dt = entry.getKey();
			}
			
			IntWritable cnt1 = new IntWritable();
			cnt1.set(maxCnt);
			String str = key.toString() + "@"+ dt;
			context.write(new Text("Date on which product " + key.toString() +
					" has been maximum searched is " + dt + " and count is "), cnt1);
			
			cnt1.set(sum);
			context.write(new Text("Total Count of the Product " + key.toString() + " searched "),cnt1);
		}
		else{
			if(countryName != null){
				count.set(sum);
				context.write(key, count);
			}
			if(searchDate != null){
				key1 = new Text("Total count of Product " + key.toString().substring(0,key.toString().indexOf("#")-1) + " searched on " + key.toString().substring(key.toString().indexOf("#")+1) + "is ");
				count.set(sum);
				context.write(key1, count);
			}
		}
	}

}
