package prv.saevel.trainings.spark.basics

import java.nio.file.Paths

trait FileUtils {

  protected def deleteFileIfExists(path: String): Boolean =
    Paths.get(System.getProperty("user.dir")).resolve(path).toFile.delete()
}
