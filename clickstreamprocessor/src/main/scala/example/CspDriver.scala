package example

import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.IntWritable

object CspDriver {
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("Usage: csp <input dir> <output dir>")
      System.exit(-1)
    }

    //we start by instantiating a Job object we can configure
    val job = Job.getInstance()
    job.setJarByClass(CspDriver.getClass())
    job.setJobName("Intro Word Count (csp)")
    job.setInputFormatClass(classOf[TextInputFormat])
    FileInputFormat.setInputPaths(job, new Path(args(0)))
    FileOutputFormat.setOutputPath(job, new Path(args(1)))

    job.setMapperClass(classOf[CspMapper])
    job.setReducerClass(classOf[CspReducer])

    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[IntWritable])

    val success = job.waitForCompletion(
      true
    )
    System.exit(if (success) 0 else 1)

  }
}
