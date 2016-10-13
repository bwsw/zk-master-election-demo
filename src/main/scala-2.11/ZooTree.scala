import java.nio.charset.Charset
import java.util.concurrent.TimeUnit

import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.framework.recipes.leader.LeaderLatch
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.zookeeper.CreateMode

import scala.collection.concurrent.TrieMap
import scala.collection.mutable.ArrayBuffer


/**
  * Created by revenskiy_ag on 12.10.16.
  */
class ZooTree(connectionString: String, val participantPathName: String) {
  private def newConnectionClient = {
    CuratorFrameworkFactory.newClient(connectionString, new ExponentialBackoffRetry(1000, 3))
  }

  private val client = {
    val clt = newConnectionClient
    clt.start()
    clt.getZookeeperClient.blockUntilConnectedOrTimedOut()
    clt
  }
  createPathIfItNotExists(participantPathName)

  def close() = client.close()

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

  def addParticipant(): String = {
    val participantId = client.create
      .withMode(CreateMode.PERSISTENT_SEQUENTIAL)
      .forPath(s"$participantPathName/",Array[Byte]())
    participantId
  }

  private var participantAgents:
  TrieMap[String, ArrayBuffer[LeaderLatchExample]] = new TrieMap[String, ArrayBuffer[LeaderLatchExample]]()

  private val connectionPerAgent:
  TrieMap[Agent, CuratorFramework] = new TrieMap[Agent, CuratorFramework]()


  def addAgentToParicipant(participantId: String, agent: Agent): Unit =
  {
    val client = if (connectionPerAgent.isDefinedAt(agent)) connectionPerAgent(agent) else {
      val clnt = newConnectionClient
      connectionPerAgent += ((agent,clnt))
      clnt.start()
      clnt.getZookeeperClient.blockUntilConnectedOrTimedOut()
      clnt
    }

      val agentPathName = s"$participantId/${agent.toString}"
      val agentPath = client.create
        .withMode(CreateMode.EPHEMERAL_SEQUENTIAL)
        .forPath(agentPathName, agent.serialize)

      val agentInVoting = new LeaderLatchExample(client, participantId, agent.toString)
      agentInVoting.start()

      if (participantAgents.isDefinedAt(participantId))
        participantAgents(participantId) += agentInVoting
      else participantAgents += ((participantId, ArrayBuffer(agentInVoting)))
  }

  def closeAgent(agent: Agent):Unit = {
    participantAgents = participantAgents.transform{(_,agentsInElection) =>
      val agentToCloseOpt = agentsInElection.find(participantAgent=> participantAgent.name == agent.toString)
      agentToCloseOpt match {
        case Some(agentToClose) => agentToClose.close; agentsInElection -= agentToClose
        case None => agentsInElection
      }
    }
    connectionPerAgent remove agent foreach(_.close())
  }

  def printParticipantAgents() = {
    participantAgents foreach{case (participantId,agents)=>
      agents foreach { agent =>
        println(s"$participantId/${agent.name}\t has leader ${agent.currentLeader.getId}")
      }
    }
  }

  def getParticipantData(participantId: String): String = {
    val data: Array[Byte] = client.getData.forPath(participantId)
    new String(data, Charset.defaultCharset())
  }
}
