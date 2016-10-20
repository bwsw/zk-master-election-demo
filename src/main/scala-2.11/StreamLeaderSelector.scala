import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.zookeeper.CreateMode

import scala.annotation.tailrec
import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class StreamLeaderSelector(connectionString: String, val rootPath: String) {
  import StreamLeaderSelector._

  val partitions: mutable.ArrayBuffer[String] = new mutable.ArrayBuffer[String]()

  def this(connectionString: String, rootPath: String, partitionNumber: Int) = {
    this(connectionString, rootPath)
    (1 to partitionNumber) foreach (_=> addPartition())
  }

  private def newConnectionClient = {
    CuratorFrameworkFactory.newClient(connectionString, new ExponentialBackoffRetry(1000, 3))
  }


  val client = {
    val clt = newConnectionClient
    clt.start()
    clt.getZookeeperClient.blockUntilConnectedOrTimedOut()
    clt
  }

  def idOfNode(path: String) = path.substring(path.lastIndexOf("/") + 1)
  val streamPathName = idOfNode(createPathIfItNotExists(rootPath))

  def close() = {
   // partitionLeaderSelectorAgents.values.foreach{agent=> agent.foreach(_.close())}
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
    partitions += partitionId
    partitionId
  }

  private val partitionLeaderSelectorAgents:
  TrieMap[String,  mutable.ArrayBuffer[MyLeaderSelectorClient]] = new TrieMap[String,  mutable.ArrayBuffer[MyLeaderSelectorClient]]()


  private def randomLeader: mutable.ArrayBuffer[MyLeaderSelectorClient] => MyLeaderSelectorClient = {leaderSelectors=>
    val index = scala.util.Random.nextInt(leaderSelectors.length)
    leaderSelectors(index)
  }


  //check if agent has a connection and exists in partition; then return Leader Selector
  private def leaderSelectorOfPartition(partitionId: String): Option[MyLeaderSelectorClient] = {
    val agentIdOpt = partitionLeaderSelectorAgents(partitionId).headOption
    agentIdOpt match {
      case Some(agentLeaderSelector) =>
        if (doesAgentHaveConnection(agentLeaderSelector.id)) Some(agentLeaderSelector) else None
      case None => None
    }
  }

  private def changeLeaderSelectorOfPartition(partitionId: String, chooseNewLeader:
  mutable.ArrayBuffer[MyLeaderSelectorClient] => MyLeaderSelectorClient): Unit =
  {
    leaderSelectorOfPartition(partitionId) foreach { leader =>
      // ???
      val leaderSelectorsOfPartition = partitionLeaderSelectorAgents(partitionId)
        .filter(agent => leaderSelectorToAgent(agent).get.priority != Agent.Priority.Low)
      val newLeader = chooseNewLeader(leaderSelectorsOfPartition)

      newLeader.requeue()
      leader.release()
    }
  }

  private def leaderSelectorToAgent(leaderSelector: MyLeaderSelectorClient): Option[Agent] = {
    connectionPerAgent.keys.find(agent => leaderSelector.id == agent.toString)
  }

  //check if agent has a connection and exists in partition; then return agent
  private def leaderAgentOfPartition(partitionId: String): Option[Agent] = {
    if (partitionLeaderSelectorAgents.isDefinedAt(partitionId)) {
      val agentIdOpt = partitionLeaderSelectorAgents(partitionId).headOption
      agentIdOpt match {
        case Some(agentLeaderSelector) => leaderSelectorToAgent(agentLeaderSelector)
        case None => None
      }
    } else None
  }

  private def geeAllLeadersOfPartitions: mutable.Map[String,Agent] = {
    val partitionLeader = mutable.Map[String, Agent]()
    partitions foreach {partition=>
      leaderAgentOfPartition(partition) foreach (agent => partitionLeader += ((partition, agent)))
    }
    partitionLeader
  }

  private def processElectionAgentByPriority(agent: Agent) = {
    def processAgent(partitionId: String, agentLeader: Agent, agent: Agent): Unit = {
      import Agent.Priority._
      (agentLeader.priority, agent.priority) match {
        case (Low, Normal) => changeLeaderSelectorOfPartition(partitionId, randomLeader)
        case _ => Unit
      }
    }
    geeAllLeadersOfPartitions foreach {case(partition,agentLeader) => processAgent(partition, agentLeader, agent)}
  }


  def addAgentToPartition(partitionId: String, agent: Agent): Unit = {
    val client = if (connectionPerAgent.isDefinedAt(agent)) connectionPerAgent(agent)
    else {
      val clnt = newConnectionClient
      connectionPerAgent += ((agent, clnt))
      clnt.start()
      clnt.getZookeeperClient.blockUntilConnectedOrTimedOut()
      clnt
    }

    val agentInVoting = new MyLeaderSelectorClient(client, partitionId, agent.toString)

    if (partitionLeaderSelectorAgents.isDefinedAt(partitionId)) {
      partitionLeaderSelectorAgents(partitionId) += agentInVoting
      processElectionAgentByPriority(agent)
    }
    else {
      partitionLeaderSelectorAgents += ((partitionId,  mutable.ArrayBuffer(agentInVoting)))
      agentInVoting.start()
    }
  }

  def closeAgent(agent: Agent):Unit = {
    partitionLeaderSelectorAgents.foreach{case(_,agentsInElection) =>
      val agentToCloseOpt = agentsInElection.find(participantAgent=> participantAgent.id == agent.toString)
      agentToCloseOpt match {
        case Some(agentToClose) => {
          agentToClose.close
          agentsInElection -= agentToClose}
        case None => agentsInElection
      }
    }
  }

  def printPatritionAgents() = {
    partitionLeaderSelectorAgents foreach{case (participantId, leaderSelector)=>
      leaderSelector foreach { agent =>
        if (agent.isStarted) println(s"$participantId/${getAgentByName(agent.id).name}\t has leader ${getAgentByName(agent.getLeader.getId).name}")
      }
      println()
    }
  }

  def isAllPatritionsAgentsHaveTheSameLeader: Boolean = {
    val agentsInVotingOfParticipants = partitionLeaderSelectorAgents.values

    @tailrec
    def helper(lst: List[MyLeaderSelectorClient], leader: MyLeaderSelectorClient): Boolean = lst match {
      case Nil => true
      case head::tail => if (head.getLeader == leader.getLeader) helper(tail,leader) else false
    }

    var isTheSameLeader = true
    for (agentsInVoting <- agentsInVotingOfParticipants) {
      val lst= agentsInVoting.toList
      isTheSameLeader = if (lst.nonEmpty) helper(lst.tail, lst.head) else true
      if (!isTheSameLeader) {isTheSameLeader = false; return isTheSameLeader}
    }
    isTheSameLeader
  }

}

object StreamLeaderSelector {
  import org.apache.curator.framework.CuratorFramework
  import scala.collection.concurrent.TrieMap

  val connectionPerAgent:
  TrieMap[Agent, CuratorFramework] = new TrieMap[Agent, CuratorFramework]()

  def getAgentByName(agentName: String): Agent = connectionPerAgent.keys.find(_.toString == agentName).get
  def doesAgentHaveConnection(agentName: String): Boolean = connectionPerAgent.keys.exists(_.toString == agentName)
}
