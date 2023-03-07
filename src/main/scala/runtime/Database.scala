package de.uni_saarland.cs.se
package runtime

import utils.ConfigurationError
import utils.{ListStorage, MapStorage, Storage, StorageType}

import scala.collection.mutable


/**
 * Configuration class for databases.
 *
 * @param read whether the database should support read operations
 * @param write whether the database should support write operations
 * @param transaction whether the database should support transactions
 * @param logging whether the database should support logging
 * @param storageType the type of storage the database should use
 */
class DatabaseConfig(
                      val read: Boolean,
                      val write: Boolean,
                      val transaction: Boolean,
                      val logging: Boolean,
                      val storageType: StorageType
                    ) {}


/**
 * A runtime-configurable version of our database SPL.
 *
 * @param config the configuration for the database
 */
class Database(val config: DatabaseConfig) {




  private var storage: Storage = scala.compiletime.uninitialized

  """
  if (isMap == true) {
    isList = false
    storage = MapStorage()
  }
  else if (isList == true) {
    isMap = false
    storage = ListStorage()
  }"""

  config.storageType match
    case StorageType.MAP => storage=MapStorage()
    case StorageType.LIST => storage=ListStorage()


  private var tmpStorage: Storage = scala.compiletime.uninitialized
  if (config.transaction) {
    tmpStorage = ListStorage()

  }

  def read(key: String): Option[String] = {
    if  (config.read){
      if (config.logging) {
        println(s"Reading value for key '$key'.")
      }
      storage.get(key)
    }
    else {
      var x: Throwable = new ConfigurationError()
      throw x
    }
  }
  def write(key: String, value: String): Unit = {
    if (!config.write) {
      var x: Throwable = new ConfigurationError()
      throw x
    }

    if (config.logging) {
      println(s"Writing value '$value' at key '$key'.")
    }
    if (config.transaction) {
      tmpStorage.put(key, value)
    }
    else {
      storage.put(key, value)
    }
  }

  def commit(): Int = {

    if(config.transaction==true && config.write==true){
      val size = tmpStorage.size()
      if(config.logging==true){
        println(s"Committing $size entries.")
      }
      tmpStorage.foreach((k, v) => storage.put(k, v))
      tmpStorage = ListStorage()
      size
    }

    else{
      var x: Throwable = new ConfigurationError()
      throw x
    }
  }

  def rollback(): Int = {
    if(config.transaction==true && config.write==true){
      val size = tmpStorage.size()
      if(config.logging==true){
        println(s"Rolling back ${tmpStorage.size()} entries.")
      }
      tmpStorage = ListStorage()
      size
    }

    else {
      var x: Throwable = new ConfigurationError()
      throw x
    }
  }

  val storageType = storage.storageType





  // TODO: implement task 1a
}