package example

import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.IntWritable


/*
*PLCM takes clickstream data, filter to only links, and produces intermediate pairs:
* prev is the resulting key, and the values is curr and n, concatenated with tab
* 
*/
class PrevLinkCountMapper extends Mapper[LongWritable, Text, Text, Text] {
  override def map(
    key: LongWritable, 
    value: Text, 
    context: Mapper[LongWritable, Text, Text, Text]#Context
    ): Unit = {
      //record contains prev, curr, type, n
      val record = value.toString().split("\\t")
      //this should be cleaner
      //we're accessing fields from the input record in order to construct the appropriate
      // intermediate k-v pair
      if(record(2) == "link"){
        context.write(
          new Text(record(0)), 
          new Text(s"${record(1)}\\t${record(3)}")
        )
      }

    }

}