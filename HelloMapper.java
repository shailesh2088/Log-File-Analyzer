package hello.world;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class HelloMapper extends Mapper<Object, Text, Text, Text> {
	private Text word = new Text();
	private StringTokenizer tokenizer;
	@Override
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		String val = "1";
		String line = value.toString();
		tokenizer = new StringTokenizer(line);
		Configuration conf = context.getConfiguration();
		String productId = conf.get("ProductId");
		String countryName = conf.get("CountryName");
		String searchDate = conf.get("SearchDate");
		while(tokenizer.hasMoreTokens()){
			String ipAddress = "";
			String logDate = "";
			String logTime = "";
			String reqURL = "";
			String country = "";
			int resCode;
			int resLength;
			String method = "";
			String url = "";
			String product = "";
			String protocol = "";
			
			if(tokenizer.hasMoreElements())
				ipAddress = tokenizer.nextToken();
			if (tokenizer.hasMoreElements())
				logDate = tokenizer.nextToken();
			if(tokenizer.hasMoreElements())
				logTime = tokenizer.nextToken();
			if(tokenizer.hasMoreElements()){
//				reqURL = tokenizer.nextToken();
//				StringTokenizer token = new StringTokenizer(reqURL);
				method = tokenizer.nextToken();
//				url = token.nextToken();
//				protocol = token.nextToken();
				//product = reqURL.substring(reqURL.indexOf("=")+1);
			}
			if(tokenizer.hasMoreElements())
			{
				url = tokenizer.nextToken();
				product = url.substring(url.indexOf("=")+1);
			}
			if(tokenizer.hasMoreElements())
				protocol = tokenizer.nextToken();
			if(tokenizer.hasMoreElements())
				country = tokenizer.nextToken();
			if(tokenizer.hasMoreElements())
				resCode = Integer.parseInt(tokenizer.nextToken());
			if (tokenizer.hasMoreElements())
				resLength = Integer.parseInt(tokenizer.nextToken());
			
			if(productId != null && searchDate == null){
				if(productId.equalsIgnoreCase(product)){
					word.set(product);
					
					context.write(word,new Text(val));
					
					//word.set(product+"@"+logDate);
					
					context.write(word,new Text(logDate));
				}
			}
			
			if(countryName != null){
				if(countryName.equalsIgnoreCase(country)){
					word.set(product);
					context.write(word, new Text(val));
				}
			}
			
			if(productId != null && searchDate != null){
				if(productId.equalsIgnoreCase(product) && searchDate.equalsIgnoreCase(logDate)){
					word.set(product+"#"+logDate);
					context.write(word, new Text(val));
				}
			}
			
		}
		
	}

}
