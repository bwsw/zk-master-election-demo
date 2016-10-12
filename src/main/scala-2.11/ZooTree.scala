import java.nio.charset.Charset
import java.util.concurrent.TimeUnit

import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.leader.LeaderLatch
import org.apache.zookeeper.CreateMode

import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Future
import scala.concurrent._
import ExecutionContext.Implicits.global

/**
  * Created by revenskiy_ag on 12.10.16.
  */
class ZooTree(client: CuratorFramework, val masterPathName: String, val participantPathName: String) {
  createPathIfItNotExists(masterPathName)
  createPathIfItNotExists(participantPathName)

  val participants: ArrayBuffer[LeaderLatch] = new ArrayBuffer[LeaderLatch]
  val masters: ArrayBuffer[String] = new ArrayBuffer[String]


  private def createPathIfItNotExists(path: String) = {
    val masterPathOpt = Option(client.checkExists().forPath(path))
    if (masterPathOpt.isDefined) path else client.create().forPath(path)
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

  def addParticipant(masterId: String): String = {
    val masterData = objectToByteArray(masterId)
    val participantId = client.create
      .withMode(CreateMode.PERSISTENT_SEQUENTIAL)
      .forPath(s"$participantPathName/",masterId.getBytes(Charset.defaultCharset()))
   // val participantLeader: LeaderLatch = new LeaderLatch(client, participantId)
    //participants += participantLeader

//    participantLeader.start()
//    participantLeader.close()
//
//    println(participantLeader.getParticipants)

    //println(participantLeader.getId)

    participantId
  }

  val participantAgents: TrieMap[String, ArrayBuffer[Agent]] = new TrieMap[String, ArrayBuffer[Agent]]()
  def addAgentToParicipant(participantId: String, agent: Agent): Unit =
  {
    val agentPathName = s"$participantId/${agent.toString}"
    val agentPath = client.create
      .withMode(CreateMode.EPHEMERAL_SEQUENTIAL)
      .forPath(agentPathName, agent.serialize)
    val agentInVoting = new LeaderLatch(client, participantId, agent.toString)
    participants += agentInVoting

    import collection.JavaConverters._


   // println()

    //Thread.sleep(1000);
   //agentInVoting.close()

    if (participantAgents.isDefinedAt(participantId))
      participantAgents(participantId) += agent
    else participantAgents += ((participantId, ArrayBuffer(agent)))
  }


  def chooseLeaderOfParcipant(participantId: String) = {
    import collection.JavaConverters._
//        val participantLeader: LeaderLatch = new LeaderLatch(client, participantId)
//         participantLeader.start()
//        participantLeader.close()
//        println(participantLeader.getLeader.getId)



//    val election = participantAgents(participantId).map(agent => new LeaderLatch(client, participantId, agent.toString))
//    election foreach { leader =>
//      leader.start()
//
//    }
//
//    Thread.sleep(10);

//    election.init foreach { leader =>
//      println(leader.getLeader.getId)
//      leader.close()
//    }

    //println(election.g)


//    election foreach { leader =>
//      for (i <- 1 to 20) {
//        println(leader.getLeader)
//        Thread.sleep(100)
//      }
//      leader.close()
//    }

//    participantLeader.start()
//    participantLeader.await()
//
//    println(participantLeader.getLeader.getId)
//
//    election foreach { leader =>
//      leader.close()
//    }
    println()
   true

  }

  def addMaster(): String = {
    val masterId = client.create.withMode(CreateMode.PERSISTENT_SEQUENTIAL).forPath(s"$masterPathName/", Array[Byte]())
    masters += masterId
    masterId
  }

  def getMasterData(masterId: String): Agent = {
    val data: Array[Byte] = client.getData.forPath(masterId)
    Agent.deserialize(data)
  }

  def setMasterData(masterId: String, agent: Agent) = {
    client.setData().forPath(masterId, agent.serialize)
  }

  def getParticipantData(participantId: String): String = {
    val data: Array[Byte] = client.getData.forPath(participantId)
    new String(data, Charset.defaultCharset())
  }

  def setParticipantData(participantId: String, masterId: String) = {
    val participantData = objectToByteArray(participantId)
    client.setData.forPath(masterId: String, participantData)
  }
}

private object ZooTree{
  val executor = java.util.concurrent.Executors.newFixedThreadPool(2)
}
