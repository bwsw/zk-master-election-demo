import scala.annotation.tailrec
import scala.collection.mutable

trait Tree {
  def addAgent(agent: Agent, streamName: String, partitions: Seq[Int])
}

class ZooTree(connectionString: String) extends Tree
{
  private val agents: mutable.ArrayBuffer[Agent]   = new mutable.ArrayBuffer[Agent]()
  val streams: mutable.ArrayBuffer[Stream] = new mutable.ArrayBuffer[Stream]()

  def addStream(name: String, partitionNumber: Int) = {
    val stream = new Stream(connectionString, s"/$name", partitionNumber)
    streams += stream
    stream
  }

  def addStream(name: String) = {
    val stream = new Stream(connectionString, s"/$name")
    streams += stream
    stream
  }

  private def idOfNode(path: String) = path.substring(path.lastIndexOf("/") + 1)
  def getStreamPartitions(streamName: String):Seq[Int] = {
    streams.find(_.streamPathName == streamName) match {
      case Some(stream) => {
        stream.partitions.map(partition=> idOfNode(partition).toInt)
      }
      case None => throw new NoSuchElementException("Stream doesn't exist!")
    }
  }

  def getStreamPartitions(stream: Stream):Seq[Int] = {
    streams.find(_ == stream) match {
      case Some(stream) => stream.partitions.map(partition=> idOfNode(partition).toInt)
      case None => throw new NoSuchElementException("Stream doesn't exist!")
    }
  }

  private def intToZooId(partition: Int): String = {
    // From 1 to 10 is length of Id in Zookeeper that CreateMode.ETHERMAL_SEQ has.
    val length = 10
    @tailrec def zeroesForPath(n: Int, acc: String): String = if (n>0) zeroesForPath(n - 1, "0" + acc) else acc
    val str = partition.toString
    zeroesForPath(length - str.length, str)
  }

  def addAgent(agent: Agent, streamName: String, partitions: Seq[Int]) = {
    val streamOpt = streams.find(_.streamPathName == streamName)
    streamOpt match {
      case None => throw new NoSuchElementException("Stream doesn't exist!")
      case Some(stream) => {
        val partitionsToProcess = partitions
          .map(partition=> s"${stream.rootPath}/${intToZooId(partition)}") intersect stream.partitions

        if (partitionsToProcess.length != partitions.length)
          throw new NoSuchElementException(s"Stream $streamName doesn't contain all of that $partitions!")
        else partitionsToProcess foreach (partition => stream.addAgentToPartition(partition,agent))
      }
    }
  }

  def closeAgent(agent:Agent): Unit = streams.foreach(stream=> stream.closeAgent(agent))

  def close() = {
    streams.foreach(_.close())
    Stream.connectionPerAgent.values.foreach(_.close())
  }

}