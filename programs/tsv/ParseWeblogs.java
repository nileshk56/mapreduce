import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CLFMapper extends Mapper<Object, Text, Text, Text>{
private SimpleDateFormat dateFormatter =
new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z");
private Pattern p =
Pattern.compile("^([\\d.]+) (\\S+) (\\S+) \\ [([\\w:/]+\\s[+\\-]\\d{4})\\] \"(\\w+) (.+?) (.+?)\" (\\d+) (\\d+)\"([^\"]+|(.+?))\" \"([^\"]+|(.+?))\"", Pattern.DOTALL);

private Text outputKey = new Text();
private Text outputValue = new Text();
@Override
protected void map(Object key, Text value, Context
context) throws IOException, InterruptedException {
String entry = value.toString();
Matcher m = p.matcher(entry);
if (!m.matches()) {
return;
}
Date date = null;
try {
date = dateFormatter.parse(m.group(4));
} catch (ParseException ex) {
return;
}
outputKey.set(m.group(1)); //ip
StringBuilder b = new StringBuilder();
b.append(date.getTime()); //timestamp
b.append('\t');
b.append(m.group(6)); //page
b.append('\t');
b.append(m.group(8)); //http status
b.append('\t');
b.append(m.group(9)); //bytes
b.append('\t');
b.append(m.group(12)); //useragent
outputValue.set(b.toString());
context.write(outputKey, outputValue);
}
}


public class ParseWeblogs extends Configured implements Tool {
public int run(String[] args) throws Exception {
Path inputPath = new Path(args[0]);
Path outputPath = new Path(args[1]);
Configuration conf = new Configuration();
Job weblogJob = new Job(conf);
weblogJob.setJobName("Weblog Transformer");
weblogJob.setJarByClass(getClass());
weblogJob.setNumReduceTasks(0);
weblogJob.setMapperClass(CLFMapper.class);
weblogJob.setMapOutputKeyClass(Text.class);
weblogJob.setMapOutputValueClass(Text.class);
weblogJob.setOutputKeyClass(Text.class);

weblogJob.setOutputValueClass(Text.class);
weblogJob.setInputFormatClass(TextInputFormat.class);
weblogJob.setOutputFormatClass(TextOutputFormat.class);
FileInputFormat.setInputPaths(weblogJob, inputPath);
FileOutputFormat.setOutputPath(weblogJob, outputPath);
if(weblogJob.waitForCompletion(true)) {
return 0;
}
return 1;
}
public static void main( String[] args ) throws Exception {
int returnCode = ToolRunner.run(new ParseWeblogs(), args);
System.exit(returnCode);
}
}

