package example

import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.IntWritable


class CspMapper extends Mapper[LongWritable, Text, Text, IntWritable] {

  override def map(
      key: LongWritable,
      value: Text,
      context: Mapper[LongWritable, Text, Text, IntWritable]#Context
  ): Unit = {
    val line = value.toString() // get input as a String

    // split lines into words, filter empty words, transform to uppercase, write each out
    line.split("\\W+").filter(_.length > 0).map(_.toUpperCase()).foreach(
      (word:String) => {context.write(new Text(word), new IntWritable(1))}
    )
    //0 hi from scala hadoop =>
    //hi 1
    //from 1
    //scala 1
    //hadoop 1
  }

}