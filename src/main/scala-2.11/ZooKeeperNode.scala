import org.apache.zookeeper.{CreateMode, ZooDefs}
import org.apache.zookeeper.data.Stat

import scala.collection.mutable

/**
  * Created by revenskiy_ag on 10.10.16.
  */
abstract class ZooKeeperNode(zoo: ZooKeeper,val name: String, val mode: CreateMode) {
  val parent: String

  protected def getNodeMetaInformation: Option[Stat] = Option(zoo.zooKeeper.exists(s"$parent/$name",false))
  protected def idOfNode(path: String) = path.substring(path.lastIndexOf("/") + 1)

  def setData(data :Data): Boolean = getNodeMetaInformation match {
    case Some(stat) => zoo.zooKeeper.setData(s"$parent/$name", data.toByteArray, stat.getVersion); true
    case None => false
  }
  def getData: Data = getNodeMetaInformation match {
      case Some(stat) => {
        val bytes:Array[Byte] = zoo.zooKeeper.getData(s"$parent/$name", false, stat)
        if (bytes.length>1) Data.serialize(bytes) else NoData
      }
      case None => throw new NoSuchElementException(s"Node: $parent/$name doesn't exist in Zookeeper!")
  }
  def create(): Unit
  def remove(): Unit = getNodeMetaInformation match {
      case Some(stat) => zoo.zooKeeper.delete(s"$parent/$name",stat.getVersion)
      case None => throw new NoSuchElementException(s"This Node: $parent/$name doesn't exist in Zookeeper!")
  }
  override def toString: String = s"$parent/$name"
}



case class PathNode(zoo: ZooKeeper, override val name: String, parent:String)
  extends ZooKeeperNode(zoo, name, CreateMode.PERSISTENT)
{
  val children: mutable.ArrayBuffer[ZooKeeperNode] = mutable.ArrayBuffer[ZooKeeperNode]()
  override def create(): Unit =
    zoo.zooKeeper.create(s"$parent/$name", Array[Byte](0), ZooDefs.Ids.OPEN_ACL_UNSAFE, mode)
}



case class MasterNode(zoo: ZooKeeper, parent: String)
  extends ZooKeeperNode(zoo, name = "", CreateMode.PERSISTENT_SEQUENTIAL)
{
  override val name = idOfNode(zoo.zooKeeper.create(s"$parent/", Array[Byte](0), ZooDefs.Ids.OPEN_ACL_UNSAFE, mode))
  override def create(): Unit = Unit
}




case class Root(zoo: ZooKeeper, participantPathName: String, masterPathName: String)
{
  val parent = ""
  val masterPath: PathNode = PathNode(zoo,masterPathName,parent)
  val participantPath: PathNode = PathNode(zoo,participantPathName,parent)
}


