package de.uni_saarland.cs.se
package traits

import utils.{ConfigurationError, ListStorage, MapStorage, Storage, StorageType}

import scala.collection.mutable


/**
 * Database interface for all database implementations and traits.
 */
trait Database {
  /**
   * The database's storage type, i.e., MAP or LIST.
   */
  val storageType: StorageType

  /**
   * Gives subclasses and traits access to the database's storage.
   *
   * @return the database's storage
   */
  protected def storage(): Storage
}

trait DatabaseConfig extends Database{
  private var storage_1: Storage = scala.compiletime.uninitialized
  var tmpStorage_1: Storage = scala.compiletime.uninitialized

  def read(key: String): Option[String]

  def write(key: String, value: String): Unit

  def commit(): Int

  def rollback(): Int
}


trait MapStoreDatabase extends DatabaseConfig{
  private var storage_1 = MapStorage()
  override val storageType: StorageType = storage_1.storageType

  override def read(key: String): Option[String] = {

    var x: Throwable = new ConfigurationError()
    throw x

  }

  override def write(key: String, value: String): Unit = {
    var x: Throwable = new ConfigurationError()
    throw x
  }

  override def commit(): Int = {
    var x: Throwable = new ConfigurationError()
    throw x
  }

  override def rollback(): Int = {
    var x: Throwable = new ConfigurationError()
    throw x
  }

  override def storage(): Storage = {
    return storage_1
  }

}
trait ListStoreDatabase extends DatabaseConfig{
  private var storage_1 = ListStorage()
  override val storageType: StorageType = storage_1.storageType

  override def read(key: String): Option[String] = {

    var x: Throwable = new ConfigurationError()
    throw x

  }

  override def write(key: String, value: String): Unit = {
    var x: Throwable = new ConfigurationError()
    throw x
  }

  override def commit(): Int = {
    var x: Throwable = new ConfigurationError()
    throw x
  }

  override def rollback(): Int = {
    var x: Throwable = new ConfigurationError()
    throw x
  }

  override def storage(): Storage = {
    return storage_1
  }

}

trait Read extends DatabaseConfig{
  private var storage_1: Storage = storage()
  override def read(key: String): Option[String] = {
    storage_1.get(key)


  }
}

trait Write extends DatabaseConfig{
  private var storage_1: Storage = storage()
  override def write(key: String, value: String): Unit = {
    storage_1.put(key, value)


  }
}

trait Transaction extends Write{
  private var storage_1: Storage = storage()
  tmpStorage_1=ListStorage()
  override def write(key: String, value: String): Unit = {
    tmpStorage_1.put(key, value)

  }


  override def commit(): Int = {
    val size = tmpStorage_1.size()

    tmpStorage_1.foreach((k, v) => storage_1.put(k, v))
    tmpStorage_1 = ListStorage()
    size

  }

  override def rollback(): Int = {
    val size = tmpStorage_1.size()

    tmpStorage_1 = ListStorage()
    size
  }
}

trait TransactionWithLogging extends Transaction{
  private var storage_1: Storage = storage()
  override def commit(): Int = {
    val size = tmpStorage_1.size()
    println(s"Committing $size entries.")
    tmpStorage_1.foreach((k, v) => storage_1.put(k, v))
    tmpStorage_1 = ListStorage()
    size

  }

  override def rollback(): Int = {
    val size = tmpStorage_1.size()
    println(s"Rolling back ${tmpStorage_1.size()} entries.")
    tmpStorage_1 = ListStorage()
    size

  }
}

trait WriteWithLogging extends Transaction{
  override def write(key: String, value: String): Unit = {

    println(s"Writing value '$value' at key '$key'.")
    tmpStorage_1.put(key, value)

  }
}

trait ReadWithLogging extends Read{
  private var storage_1: Storage = storage()
  override def read(key: String): Option[String] = {
    println(s"Reading value for key '$key'.")
    storage_1.get(key)

  }
}




// TODO: implement task 1c