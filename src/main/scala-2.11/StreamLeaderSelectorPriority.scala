import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.zookeeper.CreateMode

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class StreamLeaderSelectorPriority(connectionString: String, val rootPath: String) {
  import StreamLeaderSelectorPriority._

  implicit def agentPriorityToLeaderSelectorPriority(priority: Agent.Priority.Value) : LeaderSelectorPriority.Priority.Value = priority match {
    case Agent.Priority.Normal => LeaderSelectorPriority.Priority.Normal
    case Agent.Priority.Low => LeaderSelectorPriority.Priority.Low
  }


  val partitions: mutable.ArrayBuffer[LeaderSelectorPriority] = new mutable.ArrayBuffer[LeaderSelectorPriority]()

  def this(connectionString: String, rootPath: String, partitionNumber: Int) = {
    this(connectionString, rootPath)
    (1 to partitionNumber) foreach (_=> addPartition())
  }

  private def newConnectionClient = {
    CuratorFrameworkFactory.newClient(connectionString, new ExponentialBackoffRetry(1000, 3))
  }

  private def leaderSelectorPriorityByPath(path: String) = partitions(partitions.indexWhere(_.path == path))

  val client = {
    val clt = newConnectionClient
    clt.start()
    clt.getZookeeperClient.blockUntilConnectedOrTimedOut()
    clt
  }

  def idOfNode(path: String) = path.substring(path.lastIndexOf("/") + 1)
  val streamPathName = idOfNode(createPathIfItNotExists(rootPath))

  def close() = {
    //partitions foreach{leaderSelectorPriority=> leaderSelectorPriority.close()}
    client.close()
  }

  private def createPathIfItNotExists(path: String):String = {
    val pathOpt = Option(client.checkExists().forPath(path))
    if (pathOpt.isDefined) path
    else client.create().withMode(CreateMode.PERSISTENT).forPath(path)
  }

  def addPartition(): String = {
    val partitionId = client.create
      .withMode(CreateMode.PERSISTENT_SEQUENTIAL)
      .forPath(s"$rootPath/",Array[Byte]())
    partitions += new LeaderSelectorPriority(partitionId)
    partitionId
  }

  private val partitionAgents:
  mutable.Map[String,  mutable.ArrayBuffer[Agent]] = mutable.Map[String, mutable.ArrayBuffer[Agent]]()


  def addAgentToPartition(partitionId: String, agent: Agent): Unit = {
    val client = if (connectionPerAgent.isDefinedAt(agent)) connectionPerAgent(agent)
    else {
      val clnt = newConnectionClient
      connectionPerAgent.getOrElseUpdate(agent, clnt)
      clnt.start()
      clnt.getZookeeperClient.blockUntilConnectedOrTimedOut()
      clnt
    }


    partitions(partitions.indexWhere(_.path == partitionId)) addId (agent.toString, agent.priority, client)

    if (partitionAgents.isDefinedAt(partitionId))
      partitionAgents(partitionId) += agent
    else
      partitionAgents += ((partitionId, ArrayBuffer(agent)))
  }


  def closeAgent(agent: Agent):Unit = {
    partitions.foreach(leaderSelectorPriority => leaderSelectorPriority.removeId(agent.toString))
    partitionAgents foreach {case (_,agents) =>
      agents.find(_ == agent).foreach(agentToClose => agents -= agentToClose)
    }
  }

  def printPartitionAgents() = {
    partitionAgents foreach { case (partition, agents) =>
      val leaderSelectorPriority = leaderSelectorPriorityByPath(partition)
      agents foreach { agent =>
        println(s"$partition/${agent.name}\t has leader ${leaderSelectorPriority.getLeaderId}")
      }
      println()
    }
  }

  def getPartitionLeaderOpt(partition: String): Option[Agent] = {
    val leaderIdOpt = leaderSelectorPriorityByPath(partition).getLeaderId
    leaderIdOpt match {
      case Some(leaderId) => partitionAgents(partition).find(_.toString == leaderId)
      case None => None
    }
  }
}

object StreamLeaderSelectorPriority {
  import org.apache.curator.framework.CuratorFramework

  val connectionPerAgent: mutable.Map[Agent, CuratorFramework] = mutable.Map[Agent, CuratorFramework]()
}
