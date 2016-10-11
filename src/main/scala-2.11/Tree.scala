//
import java.net.{InetAddress, InetSocketAddress}
//import java.nio.charset.Charset
//import java.util
//
import com.twitter.common.quantity.{Amount, Time}
import com.twitter.common.zookeeper.{DistributedLockImpl, ZooKeeperClient, ZooKeeperNode, ZooKeeperUtils}
//import org.apache.zookeeper.AsyncCallback.{ChildrenCallback, StringCallback}
//import org.apache.zookeeper.KeeperException.Code
//import org.apache.zookeeper.data.Stat
//import org.apache.zookeeper.{CreateMode, WatchedEvent, Watcher, ZooDefs}
//
//
class ZooKeeper(ipAddress: InetAddress, port: Int) {
  val logger = ZooKeeper.logger

  val amount = Amount.of(100, Time.SECONDS)
  val zooKeeperClient = new ZooKeeperClient(amount, new InetSocketAddress(ipAddress,port))
  val zooKeeper = zooKeeperClient.get()
}
//
//class Stream(tree: ZooKeeper, participantName: String, masterName: String) {
//  private val masterPath = s"/$masterName"
//  private val participantPath = s"/$participantName"
//
//  def configure() = {
//    val mode = CreateMode.PERSISTENT
//
//    tree.zooKeeper.create(masterPath, Array[Byte](0), ZooDefs.Ids.OPEN_ACL_UNSAFE, mode)
//    tree.zooKeeper.create(participantPath, Array[Byte](0), ZooDefs.Ids.OPEN_ACL_UNSAFE, mode)
//  }
//
////  def getAllMasterIds: java.util.List[String] = tree.zooKeeper.getChildren(masterPath,false)
////  def getAllParticipantsIds: java.util.List[String] = tree.zooKeeper.getChildren(participantPath,false)
////  private def checkMasterAndParticipiantsNumber = getAllMasterIds.size() >= getAllParticipantsIds.size()
//
//  private def getStat(path: String,id: String) = Option(tree.zooKeeper.exists(s"$path/$id",false))
//  private def getMasterStat(id: String): Option[Stat] = getStat(masterPath, id)
//  private def getParticipantStat(id: String): Option[Stat] = getStat(participantPath, id)
//
//  def bindParticipantToMaster(participantId: String, masterId: String) = {
//    getMasterStat(masterId).isDefined && setParticipantData(participantId, masterId)
//  }
//
//  private def idOfNode(path: String) = path.substring(path.lastIndexOf("/") + 1)
//
//
//  def addIdToMaster(agentOpt: Option[Agent]) = {
//    val mode = CreateMode.PERSISTENT_SEQUENTIAL
//    agentOpt match {
//      case Some(agent) => {
//        val node = tree.zooKeeper.create(s"$masterPath/", agent.toByteArray, ZooDefs.Ids.OPEN_ACL_UNSAFE, mode)
//        idOfNode(node)
//      }
//      case None => {
//        val node = tree.zooKeeper.create(s"$masterPath/", Array[Byte](0), ZooDefs.Ids.OPEN_ACL_UNSAFE, mode)
//        idOfNode(node)
//      }
//    }
//  }
//
//  def getMasterData(id:String): Option[Agent] = {
//    getMasterStat(id) match {
//      case Some(stat) => {
//        val bytes:Array[Byte] = tree.zooKeeper.getData(s"$masterPath/$id", false, stat)
//        if (bytes.length>1) Some(Agent.serialize(bytes)) else None
//      }
//      case None => None
//    }
//  }
//  def setMasterData(id: String, agent: Agent): Boolean = {
//    getMasterStat(id) match {
//      case Some(stat) => {
//        tree.zooKeeper.setData(s"$masterPath/$id", agent.toByteArray, stat.getVersion)
//        true
//      }
//      case None => false
//    }
//  }
//
//  def getParticipantData(id:String): Option[String] = {
//    getParticipantStat(id) match {
//      case Some(stat) => {
//        val bytes: Array[Byte] = tree.zooKeeper.getData(s"$participantPath/$id", false, stat)
//        if (bytes.length > 1) Some(new String(bytes,Charset.defaultCharset())) else None
//      }
//      case None => None
//    }
//  }
//  def setParticipantData(id: String, masterId: String): Boolean = {
//    getParticipantStat(id) match {
//      case Some(stat) => {
//        tree.zooKeeper.setData(s"$participantPath/$id",masterId.getBytes(Charset.defaultCharset()), stat.getVersion)
//        true
//      }
//      case None => false
//    }
//  }
//
//  private val myWatcher:Watcher = new Watcher() {override def process(event: WatchedEvent): Unit = {
//    val a = tree.zooKeeper.getChildren(event.getPath, false)
//    new Thread(new Runnable {
//      override def run(): Unit = {
//        println(a.size())
//      }
//    }).start()
//  }}
//
//
//  private var childrenCallback: scala.collection.mutable.Seq[String] = scala.collection.mutable.Seq()
//  def lockAndOpen(path: String) = {
//    val childrenCollection = childrenCallback
//    val agents = childrenCollection.map { child =>
//      val stat = tree.zooKeeper.exists(s"$path/$child", false)
//      Agent.serialize(tree.zooKeeper.getData(s"$path/$child", false, stat))
//    }
//
//    println(childrenCollection)
//
//    val masterIdOpt = getParticipantData(idOfNode(path))
//  //  masterIdOpt match {
////      case Some(masterId) =>
////        val lockedMaster = new DistributedLockImpl(tree.zooKeeperClient, s"$masterPath/$masterId")
////        getMasterData(masterId) foreach { agentOfMaster: Agent =>
////
//////          lockedMaster.tryLock(100, java.util.concurrent.TimeUnit.MILLISECONDS)
//////          agents.head.masterAgents.withDefault(key => chooseMaster(agents, key))(masterId)
//////          lockedMaster.unlock()
//////
//////          val newMaster = getMasterData(masterId).get
//////          agents.tail foreach { agent =>
//////            lockedMaster.tryLock(100, java.util.concurrent.TimeUnit.MILLISECONDS)
//////            agent.masterAgents.withDefault(key => newMaster)(masterId) = newMaster
//////            lockedMaster.unlock()
////          }
////        }
////      case None => throw new NoSuchElementException("Participant isn't binded to one of masters")
//    }
//
//
//  private def callbackChildren: ChildrenCallback = new ChildrenCallback {
//    override def processResult(rc: Int, path: String, ctx: scala.Any, children: util.List[String]): Unit = {
//      import collection.JavaConverters._
//      childrenCallback = children.asScala
//
//      val childrenCollection = childrenCallback
////      val agents = childrenCollection.map { child =>
////        val stat = tree.zooKeeper.exists(s"$path/$child", false)
////        Agent.serialize(tree.zooKeeper.getData(s"$path/$child", false, stat))
////      }
//
//      println(childrenCollection)
//      tree.zooKeeper.getChildren(path, myWatcher)
//    }
//  }
//
//
//  def chooseMaster(masters: Seq[Agent], masterId: String) : Agent = {
//    val agentToBeMaster = masters(scala.util.Random.nextInt(masters.length))
//    if (setMasterData(masterId, agentToBeMaster)) agentToBeMaster
//    else throw new NoSuchElementException("Master doesn't exist!")
//  }
//
//  def addIdToParticipant() = {
//    val mode = CreateMode.PERSISTENT_SEQUENTIAL
//    val node = tree.zooKeeper
//      .create(s"$participantPath/", Array[Byte](0), ZooDefs.Ids.OPEN_ACL_UNSAFE, mode)
//    idOfNode(node)
//  }
//
//  def createMasterIdAndPaticipantIdAndBindThem(agent: Option[Agent]) = {
//    val participantId = addIdToParticipant()
//    val masterId     = addIdToMaster(agent)
//    val result =
//      if (bindParticipantToMaster(participantId,masterId)) (masterId, participantId)
//      else throw new Exception("Something goes wrong with the algorithm of method!")
//
//    tree.zooKeeper.getChildren(s"$participantPath/$participantId", myWatcher)
//    if (agent.isDefined) addAgentToParticipant(participantId, agent.get)
//
//    result
//  }
//
//  def addAgentToParticipant(participantId: String, agent: Agent) = {
//    val mode = CreateMode.EPHEMERAL
//    val statOpt = Option(tree.zooKeeper.exists(s"$participantPath/$participantId",false))
//    if (statOpt.isDefined) {
//      //tree.zooKeeper.getChildren(s"$participantPath/$participantId", myWatcher, callbackChildren, None)
//
//      tree.zooKeeper.create(s"$participantPath/$participantId/${agent.toString}", agent.toByteArray, ZooDefs.Ids.OPEN_ACL_UNSAFE, mode)
//      tree.zooKeeper.getChildren(s"$participantPath/$participantId", myWatcher)
//       // if (statOpt.get.getNumChildren > 0) lockAndOpen(s"$participantPath/$participantId")
//    }
//    else
//      throw new IllegalArgumentException("Participant id doesn't exist")
//  }
//}
//
private object ZooKeeper {
  import org.apache.log4j.BasicConfigurator
  import org.apache.logging.log4j.LogManager
  BasicConfigurator.configure()
  val logger = LogManager.getLogger(this)
}
