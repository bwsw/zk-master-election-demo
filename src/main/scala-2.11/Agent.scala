import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

/**
  * Created by revenskiy_ag on 12.10.16.
  */
trait Agent extends Serializable {
  val address: String
  val port: String
  val id: String

  def serialize = {
    val bytes = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(bytes)
    oos.writeObject(this); bytes.close(); oos.close()
    bytes.toByteArray
  }

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
  def deserialize(bytes: Array[Byte]): Agent = {
    val bais = new ByteArrayInputStream(bytes)
    val bytesOfObject = new ObjectInputStream(bais)
    bais.close(); bytesOfObject.readObject() match {
      case agent: Agent => bytesOfObject.close(); agent
      case _ => bytesOfObject.close(); throw new IllegalArgumentException("Object to serialize doesn't refer to Data")
    }
  }
}
