import java.io.{ByteArrayInputStream, ObjectInputStream, ObjectOutputStream}

import scala.collection.concurrent.TrieMap

/**
  * Created by revenskiy_ag on 05.10.16.
  */

trait Data extends Serializable
{
  def toByteArray = {
    val bytes = new java.io.ByteArrayOutputStream()
    val oos   = new ObjectOutputStream(bytes)
    oos.writeObject(this); oos.close()
    bytes.toByteArray
  }
}

case object NoData extends Data

object Data {
  def serialize(bytes: Array[Byte]) = {
    val bytesOfObject = new ObjectInputStream(new ByteArrayInputStream(bytes))
    bytesOfObject.readObject() match {
      case agent: Agent => agent
      case partition: Partition => partition
      case _ => throw new IllegalArgumentException("Object to serialize doesn't refer to Data")
    }
  }
}

case class Partition(id: String) extends Data
object Partition {
  def serialize(bytes: Array[Byte]) = {
    val bytesOfObject = new ObjectInputStream(new ByteArrayInputStream(bytes))
    bytesOfObject.readObject().asInstanceOf[Partition]
  }
}

trait Agent extends Data {
  val address: String
  val port: String
  val id: String
  var masterAgents: scala.collection.concurrent.TrieMap[String, Agent] = new TrieMap()

  def canEqual(other: Any): Boolean = other.isInstanceOf[Agent]

  override def hashCode(): Int = {41*(41*((41 + address.hashCode) + port.hashCode) + id.hashCode)}
  override def equals(other: scala.Any): Boolean = other match {
    case that: Agent => {
      (that canEqual this) &&
      address == that.address &&
      port == that.port &&
      id == that.id
    }
    case _ => false
  }
  override def toString: String = s"$address:$port{$id}"
}

object Agent {
  def apply(addressNew:String,portNew:String,idNew:String) = new Agent{
    val address = addressNew
    val port = portNew
    val id   = idNew
  }
  def serialize(bytes: Array[Byte]) = {
    val bytesOfObject = new ObjectInputStream(new ByteArrayInputStream(bytes))
    bytesOfObject.readObject() match {
      case agent: Agent => agent
    }
  }
}