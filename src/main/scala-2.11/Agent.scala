import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}


trait Agent extends Serializable {
  val address: String
  val port: String
  val id: String
  val priority: Agent.Priority.Value

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

  def name = s"$address:$port(version:$id) and it's priority is: $priority"
  override def toString: String = s"$address:$port(version:$id)"
}


object Agent {
  object Priority extends Enumeration {
    val Normal, Low = Value
  }

  def apply(addressNew: String, portNew: String, idNew: String,
            priorityNew: Priority.Value = Priority.Low) = new Agent
  {
    val address = addressNew
    val port = portNew
    val id   = idNew
    val priority = priorityNew
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
