import java.util.concurrent.Executors

import org.apache.zookeeper.{CreateMode, WatchedEvent, Watcher, ZooDefs}
import org.apache.zookeeper.data.Stat

import scala.collection.concurrent.TrieMap
import scala.collection.mutable



/**
  * Created by revenskiy_ag on 10.10.16.
  */

trait TraversablePath {
  val parentPath: String
  val name: String
}


abstract class ZooKeeperNode(zoo: ZooKeeper, val mode: CreateMode) extends TraversablePath {
  val parent: ZooKeeperNode
  val parentPath: String = parent.toString

  def setData(data :Data): Boolean = getNodeMetaInformation match {
    case Some(stat) => zoo.zooKeeper.setData(s"$parentPath/$name", data.toByteArray, stat.getVersion); true
    case None => false
  }
  def getData: Data = getNodeMetaInformation match {
      case Some(stat) => {
        val bytes:Array[Byte] = zoo.zooKeeper.getData(s"$parentPath/$name", false, stat)
        if (bytes.length>1) Data.serialize(bytes) else NoData
      }
      case None => throw new NoSuchElementException(s"Node: $parentPath/$name doesn't exist in Zookeeper!")
  }
  def create(): String
  def remove(): Unit = getNodeMetaInformation match {
      case Some(stat) => zoo.zooKeeper.delete(s"$parentPath/$name",stat.getVersion)
      case None => throw new NoSuchElementException(s"This Node: $parentPath/$name doesn't exist in Zookeeper!")
  }
  def bindTo(that: ZooKeeperNode): Boolean = that.setData(getData)

  protected def getNodeMetaInformation: Option[Stat] = Option(zoo.zooKeeper.exists(s"$parentPath/$name",false))
  protected def idOfNode(path: String) = path.substring(path.lastIndexOf("/") + 1)

  override def toString: String = s"$parentPath/$name"
}

case class PathNode(zoo: ZooKeeper, override val name: String, parent: ZooKeeperNode)
  extends ZooKeeperNode(zoo, CreateMode.PERSISTENT)
{
  val children: mutable.ArrayBuffer[ZooKeeperNode] = mutable.ArrayBuffer[ZooKeeperNode]()
  override def create(): String =
    zoo.zooKeeper.create(s"$parentPath/$name", Array[Byte](0), ZooDefs.Ids.OPEN_ACL_UNSAFE, mode)
}

case class MasterNode(zoo: ZooKeeper, parent: PathNode)
  extends ZooKeeperNode(zoo, CreateMode.PERSISTENT_SEQUENTIAL)
{
  override lazy val name = idOfNode(zoo.zooKeeper.create(s"$parentPath/", Array[Byte](0), ZooDefs.Ids.OPEN_ACL_UNSAFE, mode))
  override def create(): String = name
}

case class ParticipantNode(zoo: ZooKeeper, parent: PathNode, masterNode: MasterNode)
  extends ZooKeeperNode(zoo, CreateMode.PERSISTENT_SEQUENTIAL)
{
  import ParticipantNode._
  setData(Partition(masterNode.name))
  executor.submit(new Runnable {override def run(): Unit = zoo.zooKeeper.getChildren(s"$parentPath/$name", myWatcher)})

  private val children: mutable.ArrayBuffer[DataNode] = mutable.ArrayBuffer[DataNode]()

  def myWatcher: Watcher = new Watcher {
    override def process(event: WatchedEvent): Unit = {
      executor.submit(new MyCallback(zoo, s"$parentPath/$name", children, masterNode))
      executor.submit(new Runnable {override def run(): Unit = zoo.zooKeeper.getChildren(s"$parentPath/$name", myWatcher)})
    }
  }

  def addChildAndCreate(data: DataNode) = {children += data; data.create()}

  override lazy val name = idOfNode(zoo.zooKeeper.create(s"$parentPath/", Array[Byte](0), ZooDefs.Ids.OPEN_ACL_UNSAFE, mode))
  override def create(): String = name
}

private object ParticipantNode {
  val executor = Executors.newFixedThreadPool(2)
}


case class DataNode(zoo: ZooKeeper, parent: ParticipantNode, agent: Agent)
  extends ZooKeeperNode(zoo, CreateMode.EPHEMERAL)
{
  setData(agent)
  private def masterNode = parent.masterNode

  override val name: String = agent.toString
  override def create(): String = {
    idOfNode(zoo.zooKeeper.create(s"$parentPath/$name", agent.toByteArray, ZooDefs.Ids.OPEN_ACL_UNSAFE, mode))
  }
}

case class Root(zoo: ZooKeeper, participantPathName: String, masterPathName: String) extends ZooKeeperNode(zoo,CreateMode.PERSISTENT)
{
  lazy val parent = this
  val name: String = ""
  val masterPath: PathNode = PathNode(zoo, masterPathName, this)
  val participantPath: PathNode = PathNode(zoo, participantPathName, this)

  override def create(): String = ""
  override def toString: String = ""
}

