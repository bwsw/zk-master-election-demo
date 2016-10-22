import LeaderSelectorPriority.Priority
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.ExponentialBackoffRetry

import scala.collection.mutable.ArrayBuffer


class LeaderSelectorPriority(path: String) {
  val leaderSelectorsWithPriorities: ArrayBuffer[(MyLeaderSelectorClient, LeaderSelectorPriority.Priority.Value)] = new ArrayBuffer()

  def getLeader = leaderSelectorsWithPriorities.head._1.getLeader

  private def chooseLeader(currentLeader: MyLeaderSelectorClient, newLeader: MyLeaderSelectorClient) = {
    currentLeader.release()
    newLeader.requeue()
  }

  private def votersAndLeader: (
    ArrayBuffer[(MyLeaderSelectorClient, LeaderSelectorPriority.Priority.Value)],
    (MyLeaderSelectorClient, LeaderSelectorPriority.Priority.Value)
  ) = {
    val currentLeaderIndex = leaderSelectorsWithPriorities.indexWhere(_._1.hasLeadership)
    val currentLeader = leaderSelectorsWithPriorities(currentLeaderIndex)
    val votersWithoutLeader = leaderSelectorsWithPriorities - currentLeader
    (votersWithoutLeader, currentLeader)
  }

  def addId(id: String, priority: LeaderSelectorPriority.Priority.Value, client: CuratorFramework): Unit = {
    val leader = new MyLeaderSelectorClient(client,path,id)
    leaderSelectorsWithPriorities += ((leader, priority))
    if (leaderSelectorsWithPriorities.size == 1) {
      leader.start()
    } else {

      val (votersWithoutLeader, currentLeader) = votersAndLeader
      processCreate(currentLeader,(leader,priority))
    }
  }

  private def getRandomLeader(voters: ArrayBuffer[MyLeaderSelectorClient]): MyLeaderSelectorClient = {
    if (voters.nonEmpty) voters(scala.util.Random.nextInt(voters.length))
    else throw new Exception("Bug!")
  }

  def removeId(id:String):Unit = {
    leaderSelectorsWithPriorities.find(_._1.id == id) foreach {
    val (votersWithoutLeader, currentLeader) = votersAndLeader
    processDelete(votersWithoutLeader, currentLeader, getRandomLeader)
    leaderSelectorsWithPriorities -= _
    }
  }

  def processCreate(currentLeader: (MyLeaderSelectorClient, LeaderSelectorPriority.Priority.Value),
                    newLeader: (MyLeaderSelectorClient, LeaderSelectorPriority.Priority.Value)
                   ): Unit = {

    if (newLeader._2 == Priority.Normal && currentLeader._2 == Priority.Low)
      chooseLeader(currentLeader._1, newLeader._1)
  }


  def processDelete(voters: ArrayBuffer[(MyLeaderSelectorClient, LeaderSelectorPriority.Priority.Value)],
                    currentLeader: (MyLeaderSelectorClient, LeaderSelectorPriority.Priority.Value),
                    randomFunction: ArrayBuffer[MyLeaderSelectorClient] => MyLeaderSelectorClient
                   ): Unit = {
    if (voters.isEmpty) currentLeader._1.release()
    else {
      val votersNormalPriority = voters.filter(_._2 == Priority.Normal)
      val newLeader = if (votersNormalPriority.nonEmpty)
        randomFunction(votersNormalPriority.map(_._1)) else randomFunction(voters.map(_._1))

      chooseLeader(currentLeader._1, newLeader)
    }
  }

}

object LeaderSelectorPriority extends App {
  object Priority extends Enumeration {
    val Normal, Low = Value
  }


  private val CLIENT_QTY: Int = 7
  private val PATH: String = "/examples/leader0"

  val stream = new LeaderSelectorPriority(PATH)

  val connectionString = "192.168.99.100:32770"

  val clients: ArrayBuffer[CuratorFramework] = ArrayBuffer()


  (1 to CLIENT_QTY) foreach { i =>
    val client = CuratorFrameworkFactory.newClient(connectionString, new ExponentialBackoffRetry(100, 2))
    clients += client
    client.start()
    stream.addId("Client #" + i, Priority.Low, client)
  }

  val client = CuratorFrameworkFactory.newClient(connectionString, new ExponentialBackoffRetry(100, 2))
  client.start()
  stream.addId("Client #" + CLIENT_QTY+1, Priority.Normal, client)

  stream.removeId("Client #" + CLIENT_QTY+1)

  println(stream.getLeader)

  clients foreach(_.close())
}
