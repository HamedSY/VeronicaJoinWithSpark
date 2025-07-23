import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._

case class Record(id: Long, tokens: Seq[Int])

case class RecordWithPrefix(id: Long, tokens: Seq[Int], prefix: Seq[Int])

case class PrefixEntry(prefixToken: Int, recordId: Long, fullTokens: Seq[Int])



object Main extends App {

  val fileAddress = "data/bms-pos-5p-sample.txt"
  val jaccardThreshold: Double = 0.7

  val spark = SparkSession.builder()
    .appName("spark-vj")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  // Load and parse the file
  val file = spark.read.textFile(fileAddress)

  val records: Dataset[Record] = file
    .withColumn("id", monotonically_increasing_id())
    .map { row =>
      val id = row.getAs[Long]("id")
      val tokens = row.getString(0).trim.split("\\s+").map(_.toInt).distinct.sorted.toSeq
      Record(id, tokens)
    }

  // Compute global token frequencies
  val tokenFreq = records
    .flatMap(r => r.tokens.map(t => (t, 1L)))
    .groupByKey(_._1)
    .mapValues(_._2)
    .reduceGroups(_ + _)
    .map { case (token, count) => (token, count) }
    .collect()
    .toSeq
    .sortBy(_._2)
    .map(_._1)
    .zipWithIndex
    .toMap
    
  val tokenOrderBroadcast = spark.sparkContext.broadcast(tokenFreq)
  
  // Add prefixes to records
  val recordsWithPrefix = records.map { r => 
    val ordered = r.tokens.sortBy(tokenOrderBroadcast.value)
    val prefixLength = ordered.size - math.floor(ordered.size * jaccardThreshold).toInt + 1
    val prefix = ordered.take(prefixLength)
    RecordWithPrefix(r.id, ordered, prefix)
  }

  // Build inverted index (prefix -> record)
  val prefixIndex = recordsWithPrefix.flatMap { r => 
    r.prefix.map(p => PrefixEntry(p, r.id, r.tokens))
  }

  // self-join on prefix and generate candidate pairs
  val joined = prefixIndex.as("a")
    .join(
      prefixIndex.as("b"), $"a.prefixToken" === $"b.prefixToken" 
      && $"a.recordId" < $"b.recordId"
    )
    .select(
      $"a.recordId".as("id1"), $"a.fullTokens".as("tokens1"),
      $"b.recordId".as("id2"), $"b.fullTokens".as("tokens2")
    )
    .dropDuplicates("id1", "id2")

  // Define UDF for Jaccard Similarity
  val jaccardUDF = udf { (a: Seq[Int], b: Seq[Int]) =>
    val setA = a.toSet
    val setB = b.toSet
    val inter = setA.intersect(setB).size.toDouble
    val union = setA.union(setB).size.toDouble
    inter / union
  }

  val result = joined
    .withColumn("sim", jaccardUDF($"tokens1", $"tokens2"))
    .filter($"sim" >= jaccardThreshold)
    .select("id1", "id2", "sim")

  println(s"Total matching pairs: ${result.count()}")
}