import java.nio.charset.Charset

import org.apache.curator.framework.recipes.leader.LeaderLatch
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.zookeeper.CreateMode

import scala.annotation.tailrec
import scala.collection.concurrent.TrieMap
import scala.collection.mutable.ArrayBuffer


/**
  * Created by revenskiy_ag on 12.10.16.
  */
class ZooTree(connectionString: String, val partitionPathName: String) {
  private def newConnectionClient = {
    CuratorFrameworkFactory.newClient(connectionString, new ExponentialBackoffRetry(1000, 3))
  }

  private val client = {
    val clt = newConnectionClient
    clt.start()
    clt.getZookeeperClient.blockUntilConnectedOrTimedOut()
    clt
  }
  createPathIfItNotExists(partitionPathName)

  def close() = {
    client.close()
    connectionPerAgent.keys.foreach(closeAgent)
  }

  private def createPathIfItNotExists(path: String) = {
    val pathOpt = Option(client.checkExists().forPath(path))
    if (pathOpt.isDefined) path else client.create().forPath(path)
  }

  private def objectToByteArray(obj: Any): Array[Byte] = {
    val bytes = new java.io.ByteArrayOutputStream()
    val oos   = new java.io.ObjectOutputStream(bytes)
    oos.writeObject(obj); oos.close()
    bytes.toByteArray
  }

  private def deserialize(bytes: Array[Byte]): String  = {
    val bas = new java.io.ByteArrayInputStream(bytes)
    val bytesOfObject = new java.io.ObjectInputStream(bas)
    bas.close(); bytesOfObject.readObject() match {
      case obj: String => obj
      case _ => throw new IllegalArgumentException("It's not object you assume!")
    }
  }

  def addPartition(): String = {
    val patritionId = client.create
      .withMode(CreateMode.PERSISTENT_SEQUENTIAL)
      .forPath(s"$partitionPathName/",Array[Byte]())
    patritionId
  }

  private val patritionAgents:
  TrieMap[String, ArrayBuffer[LeaderLatch]] = new TrieMap[String, ArrayBuffer[LeaderLatch]]()

  private val connectionPerAgent:
  TrieMap[Agent, CuratorFramework] = new TrieMap[Agent, CuratorFramework]()


  def addAgentToPartrition(patritionId: String, agent: Agent): Unit = {
    val client = if (connectionPerAgent.isDefinedAt(agent)) connectionPerAgent(agent)
    else {
      val clnt = newConnectionClient
      connectionPerAgent += ((agent, clnt))
      clnt.start()
      clnt.getZookeeperClient.blockUntilConnectedOrTimedOut()
      clnt
    }


    val agentInVoting = new LeaderLatch(client, patritionId, agent.toString)
    agentInVoting.start()

    if (patritionAgents.isDefinedAt(patritionId))
      patritionAgents(patritionId) += agentInVoting
    else patritionAgents += ((patritionId, ArrayBuffer(agentInVoting)))
  }

  def closeAgent(agent: Agent):Unit = {
    patritionAgents.foreach{case(_,agentsInElection) =>
      val agentToCloseOpt = agentsInElection.find(participantAgent=> participantAgent.getId == agent.toString)
      agentToCloseOpt match {
        case Some(agentToClose) => {
          agentToClose.close
          agentsInElection -= agentToClose}
        case None => agentsInElection
      }
    }
    connectionPerAgent remove agent foreach(_.close)
  }

  def printPatritionAgents() = {
    patritionAgents foreach{case (participantId,agents)=>
      agents foreach { agent =>
        println(s"$participantId/${agent.getId}\t has leader ${agent.getLeader.getId}")
      }
    }
  }

  def isAllPatritionsAgentsHaveTheSameLeader: Boolean = {
    val agentsInVotingOfParticipants = patritionAgents.values

    @tailrec
    def helper(lst: List[LeaderLatch], leader: LeaderLatch): Boolean = lst match {
      case Nil => true
      case head::tail => if (head.getLeader == leader.getLeader) helper(tail,leader) else false
    }

    var isTheSameLeader = true
      for (agentsInVoting <- agentsInVotingOfParticipants) {
        val lst= agentsInVoting.toList
        isTheSameLeader = if (lst.nonEmpty) helper(lst.tail, lst.head) else true
      }
    isTheSameLeader
  }
}