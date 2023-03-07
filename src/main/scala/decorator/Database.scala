package de.uni_saarland.cs.se
package decorator

import utils.ConfigurationError
import utils.{ListStorage, MapStorage, Storage, StorageType}

import scala.collection.mutable

/**
 * Interface for the database components and decorators.
 */
trait Database {
  def read(key: String): Option[String]
  def write(key: String, value: String): Unit
  def commit(): Int
  def rollback(): Int
  val storageType: StorageType
  private[decorator] def storage(): Storage
}
abstract class AbstractDatabase(decorator: extendedDatabase) extends extendedDatabase {

  override def read(key: String): Option[String] = decorator.read(key)
  override def write(key: String, value: String): Unit = decorator.write(key, value)
  override def commit(): Int = decorator.commit()
  override def rollback(): Int = decorator.rollback()
  override def storage(): Storage = decorator.storage()
  private var storage_1: Storage = scala.compiletime.uninitialized
  var tmpStorage_1: Storage = ListStorage()

}
trait extendedDatabase extends Database{
  var is_read: Boolean
  var is_write: Boolean
  var is_logging: Boolean
  var is_transaction: Boolean
}

class MapStorageDatabase extends extendedDatabase{
  var is_read: Boolean=false
  var is_write: Boolean=false
  var is_logging: Boolean=false
  var is_transaction: Boolean=false
  private var storage_1: Storage = MapStorage()
  val storageType = storage_1.storageType

  def read(key: String): Option[String] = {

    var x: Throwable = new ConfigurationError()
    throw x

  }

  def write(key: String, value: String): Unit = {
    var x: Throwable = new ConfigurationError()
    throw x
  }

  def commit(): Int = {
    var x: Throwable = new ConfigurationError()
    throw x
  }

  def rollback(): Int = {
    var x: Throwable = new ConfigurationError()
    throw x
  }

  def storage(): Storage = {
    return storage_1
  }


}

class ListStorageDatabase extends extendedDatabase{
  var is_read: Boolean = false
  var is_write: Boolean = false
  var is_logging: Boolean = false
  var is_transaction: Boolean = false
  private var storage_1: Storage = ListStorage()
  val storageType = storage_1.storageType


  def read(key: String): Option[String] = {

    var x: Throwable = new ConfigurationError()
    throw x

  }

  def write(key: String, value: String): Unit = {
    var x: Throwable = new ConfigurationError()
    throw x
  }

  def commit(): Int = {
    var x: Throwable = new ConfigurationError()
    throw x
  }

  def rollback(): Int = {

    var x: Throwable = new ConfigurationError()
    throw x
  }

  def storage(): Storage = {
    return storage_1
  }
}

class Read(decoratorDatabase: extendedDatabase) extends AbstractDatabase(decoratorDatabase) {

  decoratorDatabase.is_read=true
  var is_read=decoratorDatabase.is_read
  var is_write=decoratorDatabase.is_write
  var is_transaction=decoratorDatabase.is_transaction
  var is_logging=decoratorDatabase.is_logging
  var storage_1 = decoratorDatabase.storage()
  val storageType = storage_1.storageType


  override def read(key: String): Option[String] = {

    if(is_read){
      storage_1.get(key)
    }
    else {
      super.read(key)
    }
  }


}
class Write(decoratorDatabase: extendedDatabase) extends AbstractDatabase(decoratorDatabase){
  decoratorDatabase.is_write = true
  var is_read = decoratorDatabase.is_read
  var is_write = decoratorDatabase.is_write
  var is_transaction = decoratorDatabase.is_transaction
  var is_logging = decoratorDatabase.is_logging
  var storage_1 = decoratorDatabase.storage()
  val storageType = storage_1.storageType

  override def write(key: String, value: String): Unit = {


    if (is_write) {
      storage_1.put(key,value)
    }
    else {
      super.write(key, value)
    }
  }
}
class Transaction(decoratorDatabase: extendedDatabase) extends AbstractDatabase(decoratorDatabase){
  decoratorDatabase.is_transaction = true
  var is_read = decoratorDatabase.is_read
  var is_write = decoratorDatabase.is_write
  var is_transaction = decoratorDatabase.is_transaction
  var is_logging = decoratorDatabase.is_logging

  var storage_1 = decoratorDatabase.storage()
  val storageType = storage_1.storageType
  //decoratorDatabase.tmpStorage_1=ListStorage()
  //tmpStorage_1 = decoratorDatabase.tmpStorage_1
  //tmpStorage_1=ListStorage()
  override def read(key: String): Option[String] = super.read(key)
  override def write(key: String, value: String): Unit = {
    if(is_write) {
      if (is_transaction) {
        tmpStorage_1.put(key, value)
      }
    }
    else {
      super.write(key, value)
    }
  }



  override def commit(): Int = {
    if(is_transaction && is_write) {
      val size = tmpStorage_1.size()

      tmpStorage_1.foreach((k, v) => storage_1.put(k, v))
      tmpStorage_1 = ListStorage()
      size
    }
    else {
      super.commit()
    }
  }

  override def rollback(): Int = {
    if(is_transaction && is_write) {

      val size = tmpStorage_1.size()

      tmpStorage_1 = ListStorage()
      size
    }
    else {
      super.rollback()
    }
  }
}
class Logging(decoratorDatabase: extendedDatabase) extends AbstractDatabase(decoratorDatabase){
  decoratorDatabase.is_logging = true
  var is_read = decoratorDatabase.is_read
  var is_write = decoratorDatabase.is_write
  var is_transaction = decoratorDatabase.is_transaction
  var is_logging = decoratorDatabase.is_logging
  var storage_1 = decoratorDatabase.storage()
  //tmpStorage_1 = decoratorDatabase.tmpStorage_1
  //tmpStorage_1=ListStorage()
  val storageType = storage_1.storageType
  override def read(key: String): Option[String] = {
    if(is_read) {
      if (is_logging) {
        println(s"Reading value for key '$key'.")

      }
      storage_1.get(key)
    }
    else {
      super.read(key)
    }
  }

  override def write(key: String, value: String): Unit = {
    if(is_write && is_transaction) {
      if (is_logging) {
        println(s"Writing value '$value' at key '$key'.")

      }
      tmpStorage_1.put(key, value)
    }
    else {
      super.write(key, value)
    }
  }

  override def commit(): Int = {
    if(is_write && is_transaction) {
      val size = tmpStorage_1.size()
      if (is_logging) {

        println(s"Committing $size entries.")

      }
      tmpStorage_1.foreach((k, v) => storage_1.put(k, v))
      tmpStorage_1 = ListStorage()
      size
    }
    else {
      super.commit()
    }

  }

  override def rollback(): Int = {
    if(is_write && is_transaction){
      val size = tmpStorage_1.size()
      if(is_logging) {

        println(s"Rolling back ${tmpStorage_1.size()} entries.")

      }
      tmpStorage_1 = ListStorage()
      size
    }
    else {
      super.rollback()
    }
  }
}
// TODO: implement task 1b