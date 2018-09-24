package prv.saevel.trainings.spark.basics

import java.nio.file.{Files, Path, Paths}

trait FileUtils {

  protected def deleteFileIfExists(path: String): Boolean =
    Paths.get(System.getProperty("user.dir")).resolve(path).toFile.delete()

  protected def deleteDirectoryIfExists(path: String): Boolean = {
    val directoryPath: Path = Paths.get(System.getProperty("user.dir")).resolve(path)

    directoryPath.toFile.listFiles.foreach(file =>
      if(file isDirectory){
        deleteDirectoryIfExists(file.getPath)
      } else {
        deleteFileIfExists(file.getPath)
      }
    )

    Files.deleteIfExists(directoryPath)
  }

}
