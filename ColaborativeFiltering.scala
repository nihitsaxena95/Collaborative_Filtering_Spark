package com.recommend


import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.io.Source
import java.nio.charset.CodingErrorAction
import scala.io.Codec
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import scala.math.sqrt

object ColaborativeFiltering {
  
  // com.recommend.ColaborativeFiltering
  type MovieRating = (Int, Double)
  type UserRatingPair = (Int, (MovieRating, MovieRating))
  
  type RatingPair = (Double, Double)
  type RatingPairs = Iterable[RatingPair]
  
  type UsrRate = (Int, MovieRating)
  
  type MappedMovieRating = ((Int, Int), RatingPair)
  
  def mapMovieNames():Map[Int,String] = {
    
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)
    
    var movName:Map[Int, String] = Map()
    
    val lines = Source.fromFile("../ml-latest-small/movies.csv").getLines()
    
    for(line <- lines) {
      val li = line.split(",")
      if(li.length > 0) {
        movName += (li(0).toInt -> li(1))
      }
    }
    
    return movName
  }
  
  def userBasedMaping(lines:String):UsrRate = {
    val line = lines.split(",")
    val usr = line(0).toInt
    val mov = line(1).toInt
    val rat = line(2).toDouble
    
    return (usr, (mov, rat))
  } 
  
  def filterDuplicate(userRatings:UserRatingPair):Boolean = {
    val movieR1 = userRatings._2._1
    val movieR2 = userRatings._2._2
    
    val mov1 = movieR1._1
    val mov2 = movieR2._2
    
    return mov1 != mov2
  }
  
  def mappPair(userRatings:UserRatingPair):MappedMovieRating = {
    val movieR1 = userRatings._2._1
    val movieR2 = userRatings._2._2
    
    val mov1 = movieR1._1
    val rate1 = movieR1._2
    val mov2 = movieR2._1
    val rate2 = movieR2._2
    
    return ((mov1, mov2), (rate1, rate2))
  }
  
  def computeCosineSimilarity(ratingPairs:RatingPairs): (Double, Int) = {
    var numPairs:Int = 0
    var sum_xx:Double = 0.0
    var sum_yy:Double = 0.0
    var sum_xy:Double = 0.0
    
    for (pair <- ratingPairs) {
      val ratingX = pair._1
      val ratingY = pair._2
      
      sum_xx += ratingX * ratingX
      sum_yy += ratingY * ratingY
      sum_xy += ratingX * ratingY
      numPairs += 1
    }
    
    val numerator:Double = sum_xy
    val denominator = sqrt(sum_xx) * sqrt(sum_yy)
    
    var score:Double = 0.0
    if (denominator != 0) {
      score = numerator / denominator
    }
    
    return (score, numPairs)
  }
    
  def main(args: Array[String]): Unit = {
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    
     // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "MovieSimilarities")
    
    println("\nLoading movie names...")
    val nameDict = mapMovieNames()
    
    val data = sc.textFile("../ml-latest-small/ratings.csv")
    
    //read and map movie data based on (userId, (movId, rating)) 
    val usrRating = data.map(userBasedMaping)
    
    //emit every movie rated by same user
    val joinedRate = usrRating.join(usrRating)
    
    //filter out duplicates from above join
    val uniqueRate = joinedRate.filter(filterDuplicate)
    
    // now map data into movie pairs to find similarity (mov1, mov2) => (rate1, rate2)
    val mapPairs = uniqueRate.map(mappPair)
    
    // grouped by movie pair ratings (mov1, mov2) => [(rate1, rate2), (rate1, rate2), ...
    val groupByMoviePair = mapPairs.groupByKey()
    
    //Calculate similarity based on rating vector
    val moviePairSimilarity = groupByMoviePair.mapValues(computeCosineSimilarity).cache()
    
    //Sort similarities if req
    //val sortedmovies = moviePairSimilarity.sortByKey()
    
    if (args.length > 0) {
      val scoreThreshold = 0.97
      val coOccurenceThreshold = 50.0
      
      val movieID:Int = args(0).toInt
      
      // Filter for movies with this sim that are "good" as defined by
      // our quality thresholds above     
      
      val filteredResults = moviePairSimilarity.filter( x =>
        {
          val pair = x._1
          val sim = x._2
          (pair._1 == movieID || pair._2 == movieID) && sim._1 > scoreThreshold && sim._2 > coOccurenceThreshold
        }
      )
        
      // Sort by quality score.
      val results = filteredResults.map( x => (x._2, x._1)).sortByKey(false).take(10)
      
      println("\nTop 10 similar movies for " + nameDict(movieID))
      for (result <- results) {
        val sim = result._1
        val pair = result._2
        // Display the similarity result that isn't the movie we're looking at
        var similarMovieID = pair._1
        if (similarMovieID == movieID) {
          similarMovieID = pair._2
        }
        println(nameDict(similarMovieID) + "\tscore: " + sim._1 + "\tstrength: " + sim._2)
      }
    }
    
  }
}