package org.example

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, FileUtil, Path}

import java.net.URI


object Main extends App {

  val conf = new Configuration()
  val fs = FileSystem.get(new URI("hdfs://localhost:9000"), conf, "root")

  processDirectory("/stage", "/ods")

  fs.delete(new Path("/stage"), true)

  def createDirectory(folderPath: String): Unit = {
    val path = new Path(folderPath)
    if (!fs.exists(path)) {
      fs.mkdirs(path)
    }
  }

  def processDirectory(from: String, to: String): Unit = {

    createDirectory(to)

    val fromPath = new Path(from)
    val listStatus = fs.listStatus(fromPath)

    val files = listStatus.filter(_.isFile)
    copyFiles(files, to)

    val directories = listStatus.filter(_.isDirectory)
    copyDirectories(directories, to)

  }

  def copyFiles(files: Array[FileStatus], to: String): Unit = {
    if (!files.isEmpty) {

      val srcs = files.map(file => new Path(file.getPath.toUri.getPath))
      val dst = new Path(to)

      FileUtil.copy(fs, srcs, fs, dst, false, false, conf)

      val listStatus = fs.listStatus(dst).filter(_.isFile)
      val (file :: tail) = listStatus.toList

      if (!tail.isEmpty) {
        fs.concat(file.getPath, tail.map(_.getPath).toArray)
      }

    }

  }

  def copyDirectories(directories: Array[FileStatus], to: String): Unit = {
    for (directory <- directories) {
      val path = to + "/" + directory.getPath.getName
      processDirectory(directory.getPath.toUri.getPath, path)
    }
  }

}
