//import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
//import org.apache.curator.retry.ExponentialBackoffRetry
//import org.apache.zookeeper.CreateMode
//
//import scala.annotation.tailrec
//import scala.collection.concurrent.TrieMap
//import scala.collection.mutable
//import scala.collection.mutable.ArrayBuffer
//
//class StreamLeaderSelector(connectionString: String, val rootPath: String) {
//  import StreamLeaderSelector._
//
//  val partitions: mutable.ArrayBuffer[String] = new mutable.ArrayBuffer[String]()
//
//  def this(connectionString: String, rootPath: String, partitionNumber: Int) = {
//    this(connectionString, rootPath)
//    (1 to partitionNumber) foreach (_=> addPartition())
//  }
//
//  private def newConnectionClient = {
//    CuratorFrameworkFactory.newClient(connectionString, new ExponentialBackoffRetry(1000, 3))
//  }
//
//
//  val client = {
//    val clt = newConnectionClient
//    clt.start()
//    clt.getZookeeperClient.blockUntilConnectedOrTimedOut()
//    clt
//  }
//
//  def idOfNode(path: String) = path.substring(path.lastIndexOf("/") + 1)
//  val streamPathName = idOfNode(createPathIfItNotExists(rootPath))
//
//  def close() = {
//   // partitionLeaderSelectorAgents.values.foreach{agent=> agent.foreach(_.close())}
//    client.close()
//  }
//
//  private def createPathIfItNotExists(path: String):String = {
//    val pathOpt = Option(client.checkExists().forPath(path))
//    if (pathOpt.isDefined) path
//    else client.create().withMode(CreateMode.PERSISTENT).forPath(path)
//  }
//
//  def addPartition(): String = {
//    val partitionId = client.create
//      .withMode(CreateMode.PERSISTENT_SEQUENTIAL)
//      .forPath(s"$rootPath/",Array[Byte]())
//    partitions += partitionId
//    partitionId
//  }
//
//  private val partitionLeaderSelectorAgents:
//  TrieMap[String,  mutable.ArrayBuffer[MyLeaderSelectorClient]] = new TrieMap[String,  mutable.ArrayBuffer[MyLeaderSelectorClient]]()
//
//
//
//
//  //check if agent has a connection and exists in partition; then return Leader Selector
//  private def masterLeaderSelectorOfPartition(partitionId: String): Option[MyLeaderSelectorClient] = {
//    val agentIdOpt = partitionLeaderSelectorAgents(partitionId).find(_.hasLeadership == true)
//    agentIdOpt match {
//      case Some(agentLeaderSelector) =>
//        if (doesAgentHaveConnection(agentLeaderSelector.id)) Some(agentLeaderSelector) else None
//      case None => None
//    }
//  }
//
//  private def leaderSelectorToAgent(leaderSelector: MyLeaderSelectorClient): Option[Agent] = {
//    connectionPerAgent.keys.find(agent => leaderSelector.id == agent.toString)
//  }
//
//  private def getAllLeaderSelectorsOfPartitionByPriority(partitionId: String,
//                                                 priority: Agent.Priority.Value): ArrayBuffer[MyLeaderSelectorClient] =
//  {
//    partitionLeaderSelectorAgents(partitionId).filter(agent => leaderSelectorToAgent(agent).get.priority == priority)
//  }
//
//
//
//  private def changeLeaderSelectorOfPartition(partitionId: String, chooseNewLeader:
//  mutable.ArrayBuffer[MyLeaderSelectorClient] => MyLeaderSelectorClient, priority: Agent.Priority.Value): Unit =
//  {
//    masterLeaderSelectorOfPartition(partitionId) foreach { leader =>
//      val newLeader = chooseNewLeader(getAllLeaderSelectorsOfPartitionByPriority(partitionId, priority))
//      newLeader.requeue()
//      leader.release()
//    }
//  }
//
//
//  //check if agent has a connection and exists in partition; then return agent
//  private def leaderAgentOfPartition(partitionId: String): Option[Agent] = {
//    if (partitionLeaderSelectorAgents.isDefinedAt(partitionId)) {
//      val agentIdOpt = partitionLeaderSelectorAgents(partitionId).find(_.hasLeadership == true)
//      agentIdOpt match {
//        case Some(agentLeaderSelector) => leaderSelectorToAgent(agentLeaderSelector)
//        case None => None
//      }
//    } else None
//  }
//
//  private def geeAllLeadersOfPartitions: mutable.Map[String,Agent] = {
//    val partitionLeader = mutable.Map[String, Agent]()
//    partitions foreach {partition=>
//      // foreach for Option isn't obvious
//      leaderAgentOfPartition(partition) foreach (agent => partitionLeader += ((partition, agent)))
//    }
//    partitionLeader
//  }
//
//  private def randomLeader: mutable.ArrayBuffer[MyLeaderSelectorClient] => MyLeaderSelectorClient = {leaderSelectors=>
//    val index = scala.util.Random.nextInt(leaderSelectors.length)
//    leaderSelectors(index)
//  }
//
//  private def processElectionAgentByPriority(agent: Agent) = {
//    def processAgent(partitionId: String, agentLeader: Agent, agent: Agent): Unit = {
//      import Agent.Priority._
//      (agentLeader.priority, agent.priority) match {
//        case (Low, Normal) => changeLeaderSelectorOfPartition(partitionId, randomLeader, Agent.Priority.Normal)
//        case _ => Unit
//      }
//    }
//    geeAllLeadersOfPartitions foreach {case(partition,agentLeader) => processAgent(partition, agentLeader, agent)}
//  }
//
//
//  def addAgentToPartition(partitionId: String, agent: Agent): Unit = {
//    val client = if (connectionPerAgent.isDefinedAt(agent)) connectionPerAgent(agent)
//    else {
//      val clnt = newConnectionClient
//      connectionPerAgent += ((agent, clnt))
//      clnt.start()
//      clnt.getZookeeperClient.blockUntilConnectedOrTimedOut()
//      clnt
//    }
//
//    val agentInVoting = new MyLeaderSelectorClient(client, partitionId, agent.toString)
//
//    if (partitionLeaderSelectorAgents.isDefinedAt(partitionId)) {
//      partitionLeaderSelectorAgents(partitionId) += agentInVoting
//      processElectionAgentByPriority(agent)
//    }
//    else {
//      partitionLeaderSelectorAgents += ((partitionId,  mutable.ArrayBuffer(agentInVoting)))
//      agentInVoting.start()
//    }
//  }
//
//
//  def closeAgent(agent: Agent):Unit = {
//    partitionLeaderSelectorAgents.foreach { case (partitionId, leaderSelectorsInVoting) =>
//      val leaderSelectorToCloseOpt = leaderSelectorsInVoting.find(participantAgent => participantAgent.id == agent.toString)
//      leaderSelectorToCloseOpt foreach { leaderSelectorToClose =>
//
//        if (leaderSelectorToClose.hasNotLeadership) {
//          leaderSelectorsInVoting -= leaderSelectorToClose
//          leaderSelectorToClose.close
//        } else if (leaderSelectorToAgent(leaderSelectorToClose).get.priority == Agent.Priority.Normal) {
//
//        }
//          val leaderSelectorsWithoutMaster = leaderSelectorsInVoting - leaderSelectorToClose
//
//          def test(priority: Agent.Priority.Value) =
//            leaderSelectorsWithoutMaster.filter(x=>leaderSelectorToAgent(x).get.priority == priority)
//
//          val newLeader = if (test(Agent.Priority.Normal).isEmpty)
//            randomLeader(test(Agent.Priority.Low))
//          else randomLeader(test(Agent.Priority.Normal))
//
//          newLeader.requeue()
//          leaderSelectorToClose.release()
//
//          leaderSelectorsInVoting -= leaderSelectorToClose
//          leaderSelectorToClose.close
//      }
//    }
//  }
//
//  def printPatritionAgents() = {
//    partitionLeaderSelectorAgents foreach { case (participantId, leaderSelector) =>
//      leaderSelector foreach { agentLeaderSelector =>
//        println(s"$participantId/${getAgentByName(agentLeaderSelector.id).name}\t has leader ${getAgentByName(agentLeaderSelector.getLeader.getId).name}")
//      }
//      println()
//    }
//  }
//
//  def isAllPatritionsAgentsHaveTheSameLeader: Boolean = {
//    val agentsInVotingOfParticipants = partitionLeaderSelectorAgents.values
//
//    @tailrec
//    def helper(lst: List[MyLeaderSelectorClient], leader: MyLeaderSelectorClient): Boolean = lst match {
//      case Nil => true
//      case head::tail => if (head.getLeader == leader.getLeader) helper(tail,leader) else false
//    }
//
//    var isTheSameLeader = true
//    for (agentsInVoting <- agentsInVotingOfParticipants) {
//      val lst= agentsInVoting.toList
//      isTheSameLeader = if (lst.nonEmpty) helper(lst.tail, lst.head) else true
//      if (!isTheSameLeader) {isTheSameLeader = false; return isTheSameLeader}
//    }
//    isTheSameLeader
//  }
//
//}
//
//object StreamLeaderSelector {
//  import org.apache.curator.framework.CuratorFramework
//  import scala.collection.concurrent.TrieMap
//
//  val connectionPerAgent:
//  TrieMap[Agent, CuratorFramework] = new TrieMap[Agent, CuratorFramework]()
//
//  def getAgentByName(agentName: String): Agent = connectionPerAgent.keys.find(_.toString == agentName).get
//  def doesAgentHaveConnection(agentName: String): Boolean = connectionPerAgent.keys.exists(_.toString == agentName)
//}
