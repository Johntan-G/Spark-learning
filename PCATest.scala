import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.{Matrix, Vector, Vectors}
import org.apache.spark.mllib.linalg.distributed._
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}

import scala.util.control._
import String._


object PCATest {

    def preprocess(line: String): Vector = {
        var line1: Array[String] = line.split(",").filter((x: String) => x != "null")
        var line2: Array[Double] = new Array[Double](line1.length)
        if (line1.length != 54676)
            line2 = Array(1.0)
        else {
            var count = 0
            val loop = new Breaks
            loop.breakable {
                for (i <- Range(0, 54676)) {
                    try {
                        line2(i) = line1(i).replaceAll("\"", "").toDouble
                    } catch {
                        case ex: java.lang.NumberFormatException => {
                            count = count + 1
                        }
                    }
                    if (count > 1) {
                        line2 = Array(1.0)
                        loop.break
                    }
                }
            }

        }
        Vectors.dense(line2)
    }

    def pearson(dataRDD: RDD[Vector]): CoordinateMatrix = {


    }
}




    def main(args: Array[String]) {
        val conf = new SparkConf().setAppName("PCATest")
        val sc = new SparkContext(conf)

        val path = "hdfs:///user/hadoopuser/ExpAltas/Splited/GPL570.Splited00"
        //        var dataRDD: RDD[Vector] = sc.objectFile(path).repartition(args(1).toInt)
        var dataRDD: RDD[Vector] = sc.textFile(path).map(preprocess).filter(row => row.size != 1)
        var dataRDD: RDD[Vector] = sc.parallelize(
            Array(Vectors.dense(1.0, 0.0, 2.0, 0.0),
                  Vectors.dense(2.0, 5.0, 1.0, 3.0),
                  Vectors.dense(4.0 ,5.0 ,6.0 ,1.0))
        )


        val start = System.nanoTime()
        val summary: MultivariateStatisticalSummary = Statistics.colStats(dataRDD)
        val mean: Vector = summary.mean
        val variance = summary.variance
        val factor = summary.count - 1.0
        //println("***vince:mean:" + mean  + ",variance count:" + variance.size)

        // transform to coordinate Martix in case of OutofMemory
        val zeroRDD = new IndexedRowMatrix(dataRDD.map(
            (row:Vector) => Vectors.dense(row.toArray.map(
                x => if(x == 0) 1.0 else 0 ))).zipWithIndex().map(x => IndexedRow(x._2, x._1))).toBlockMatrix(1024,1024).toCoordinateMatrix


        val coordinateRDD = new IndexedRowMatrix(dataRDD.zipWithIndex().map(x => IndexedRow(x._2, x._1))).toBlockMatrix(1024,1024).toCoordinateMatrix


        // Normalization
        val NormalizedRDD = new CoordinateMatrix(coordinateRDD.entries
          .map(x => MatrixEntry(x.i, x.j, x.i (x.value - mean(x.j.toInt))  ))
        )

        val complementRDD = zeroRDD.entries.collect.foreach { case MatrixEntry(i, j, value) =>



            /* It seem strange in here
            if I execute the following direction
                    val NormalizedRDD = new CoordinateMatrix(coordinateRDD.entries
                        .map(x => MatrixEntry(x.i, x.j,  (x.value - mean(x.j.toInt)) ))
                    )
                    NormalizedRDD.toBlockMatrix.toLocalMatrix
            It will output:
                    res1: org.apache.spark.mllib.linalg.Matrix =
                    -1.3333333333333335  0.0                -1.0  1.3333333333333335
                    -0.3333333333333335  1.666666666666667  -2.0  0.3333333333333335
                    1.6666666666666665   1.666666666666667  3.0   -1.6666666666666665
             where the definitely correct answer in R is
                    array([[-1.33333333, -3.33333333, -1.        ,  1.33333333],
                           [-0.33333333,  1.66666667, -2.        ,  0.33333333],
                           [ 1.66666667,  1.66666667,  3.        , -1.66666667]])
             The only difference between these two matrix is the entry in the first row and second coloumn
             I can't figure out the reason under this phenomena
             Again, thanks vince' help in advance.
            */

        // computer the pearson correlation coefficient
        val PearsonRDD = NormalizedRDD.toBlockMatrix.transpose.multiply(NormalizedRDD.toBlockMatrix)



        //        val rdd_vec: RDD[Vector] = dataRDD.map(row => {
        //          Vectors.dense(row.toArray.zipWithIndex.map(x => {
        //            val a: Double = (x._1 - mean(x._2))/math.sqrt(variance(x._2).toDouble * number)
        //      a
        //          }))
        //        })
        //        val data = new IndexedRowMatrix(rdd_vec.zipWithIndex().map(x=>IndexedRow(x._2, x._1))).toBlockMatrix(1024,1024)
        //    val cov1 = data.transpose.multiply(data).toLocalMatrix

        //compte pearson matrix
        //    val cov1 = data.transpose.multiply(data).toCoordinateMatrix






        ////compute pearson matrix
        //    val pearM = new CoordinateMatrix(cov1.entries
        //      .map(x => MatrixEntry(x.i, x.j, if (x.i == x.j) 1.0 else x.value * m /(math.sqrt(v(x.j.toInt)) * math.sqrt(v(x.i.toInt)))))
        //    )
        //println("****vince:result rows:" + pearM.numRows + ", cols:" + pearM.numCols )

        /*
        // validate the result by comparing the data output from Spark 'corr' API on original input
            import breeze.linalg.{DenseMatrix => BDM}
            val matP = BDM.zeros[Double](pearM.numRows.toInt, pearM.numCols.toInt)
            pearM.entries.collect().foreach { case MatrixEntry(i, j, value) =>
              matP(i.toInt, j.toInt) = value
            }
            println(s"****vin:return pearson matrix 1: \n${matP}")

           val correlMatrix: Matrix = Statistics.corr(dataRDD, "pearson")
           println(s"***return spark corr: \n{correlMatrix.toString}")
        */
        println(s"****compute done****")
        val time = (System.nanoTime() - start) / 1e9
        sc.stop()
    }
}
