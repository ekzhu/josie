import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel

import scala.reflect.ClassTag
import scala.util.hashing.MurmurHash3

// This is the Spark job used to measure the indexing process
// for the SIGMOD revision.
// Must run under ekzhu on cluster.
object CreateIndex {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("CreateIndex")
      .set("spark.executor.memory", "100g")
      .set("spark.driver.memory", "5g")
    val sc = new SparkContext(conf)

    // The input sets, preprocessed to remove numerical values
    val setFile = "canada_us_uk_opendata.set"
    // The output sets and posting lists.
    val set2File = "canada_us_uk_opendata.set-2"
    val listFile = "canada_us_uk_opendata.inverted-list"
    // The tokens to skip.
    val skipTokens = Set("acssf", "-", "*", "total", "n/a", "..")

    // For web table
    // val setFile = "wdc_webtables_2015_english_relational.set"
    // val set2File = "wdc_webtables_2015_english_relational.sets-2"
    // val listFile = "wdc_webtables_2015_english_relational.inverted-list"
    // val skipTokens = Set("-", "--", "n/a", "total", "$", ":", "*", "+", "�", "@", "†", "▼")

    // Stage 1: Build Token Table
    // Load sets and filter out removed tokens
    val sets = sc.textFile(setFile).map(line => line.split(" "))
      .map(line => (line(0).toInt, line.drop(1).filter(
        token => !skipTokens(token))))
    // Create token to set pairs
    val tokenSets = sets.flatMap { case (sid, tokens) =>
      tokens.map(token => (token, sid))
    }
    // Create posting lists and the global ordering indexes for all tokens.
    // The global ordering index is sorted by
    // 1) token frequency,
    // 2) hash value,
    // 3) its posting list of sorted set ids.
    object PostingListOrdering extends Ordering[(Int, Array[Int])] {
      override def compare(a: (Int, Array[Int]), b: (Int, Array[Int])): Int = {
        if (a._2.length != b._2.length) a._2.length.compareTo(b._2.length)
        else if (a._2.isEmpty || b._2.isEmpty) 0
        else if (a._1 != b._1) a._1.compareTo(b._1)
        else {
          for ((x, y) <- a._2.zip(b._2)) {
            if (x != y) {
              return x.compareTo(y)
            }
          }
          return 0
        }
      }
    }
    val postingListsSorted = tokenSets.groupByKey().map {
      case (token, sids) => (token, sids.toArray.sorted)
    }.map{
      case (token, sids) => (token, sids, MurmurHash3.arrayHash(sids))
    }.sortBy {
      case (_, sids, hash) => (hash, sids)
    }(PostingListOrdering, implicitly[ClassTag[(Int, Array[Int])]]).zipWithIndex().map {
      case ((rawToken, sids, hash), tokenIndex) => (tokenIndex, (rawToken, sids, hash))
    }.persist(StorageLevel.MEMORY_ONLY_SER)
    // Create the duplicate groups.
    val duplicateGroupIDs = postingListsSorted.map {
      case (tokenIndex, (_, sidsLower, hash)) => (tokenIndex + 1, (sidsLower, hash))
    }.join(postingListsSorted).map {
      case (tokenIndexUpper, ((sidsLower, hashLower), (_, sidsUpper, hashUpper))) => {
        // If the lower and upper posting lists are different, then the upper posting
        // list belongs to a new group, and the upper token index is the new group's
        // starting index.
        if (hashLower == hashUpper && java.util.Arrays.equals(sidsLower, sidsUpper)) (-1) else (tokenIndexUpper)
      }
      // Add the first group's starting index, which is 0, and then create the
      // group IDs.
    }.filter(i => i > 0).union(sc.parallelize(List(0))).sortBy(i => i).zipWithIndex().map {
      // Returns a mapping from group ID to the starting index of the group.
      case (startingIndex, gid) => (gid, startingIndex)
    }
    // Generating all token indexes of each group.
    val tokenGroupIDs = duplicateGroupIDs.join(duplicateGroupIDs.map {
      case (gid, startingIndexUpper) => (gid - 1, startingIndexUpper)
    }).flatMap {
      case (gid, (startingIndexLower, startingIndexUpper)) =>
        (startingIndexLower until startingIndexUpper).map(tokenIndex => (tokenIndex, gid))
    }.persist(StorageLevel.MEMORY_ONLY_SER)
    // Join posting lists with their duplicate group IDs
    val postingListsWithGroupIDs = postingListsSorted.join(tokenGroupIDs).map {
      case (tokenIndex, ((rawToken, sids, _), gid)) => (tokenIndex, (gid, rawToken, sids))
    }

    // Stage 2: Create Integer Sets
    // Create sets and replace text tokens with token index
    val integerSets = postingListsWithGroupIDs.flatMap {
      case (tokenIndex, (_, _, sids)) => sids.map(sid => (sid, tokenIndex))
    }.groupByKey().map {
      case (sid, tokenIndexes) => (sid, tokenIndexes.toArray.sorted)
    }

    // Stage 3: Create the final posting lists
    // Create new posting lists and join with the previous inverted lists to
    // obtain the final posting lists with all the information
    val postingLists = integerSets.flatMap {
      case (sid, tokens) => tokens.zipWithIndex.map {
        case (token, position) => (token, (sid, tokens.length, position))
      }
    }.groupByKey().map {
      case (token, sets) => (token, sets.toArray.sortBy { case (sid, _, _) => sid })
    }.join(postingListsWithGroupIDs).map {
      case (token, (sets, (gid, rawToken, _))) => (token, rawToken, gid, sets)
    }

    // Stage 4: Save integer sets and final posting lists
    integerSets.map {
      case (sid, indices) => s"$sid ${indices.mkString(" ")}"
    }.saveAsTextFile(set2File)
    postingLists.map {
      case (token, rawToken, gid, sets) =>
        s"$token $rawToken $gid ${sets.mkString(" ")}"
    }.saveAsTextFile(listFile)
  }
}
