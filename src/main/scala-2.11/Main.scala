import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.framework.recipes.leader.LeaderLatch
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.curator.{RetryPolicy, RetrySleeper}
import org.apache.zookeeper.CreateMode
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Created by revenskiy_ag on 12.10.16.
  */
object Main extends App{
  val zookeeperConnectionString = "172.17.0.2:2181"

  val zoo = new ZooTree(zookeeperConnectionString, "/participant")

  val participant1 = zoo.addParticipant
  val participant2 = zoo.addParticipant


  val agent1 = Agent("192.168.0.1","1111","1")
  val agent2 = Agent("192.168.0.2","2222","1")
  val agent3 = Agent("192.168.0.3","3333","1")
  val agent4 = Agent("192.168.0.4","4444","1")
  val agent5 = Agent("192.168.0.5","5555","1")
  val agent6 = Agent("192.168.0.6","6666","1")
  val agent7 = Agent("192.168.0.7","7777","1")

  zoo.addAgentToParicipant(participant1,agent1)
  zoo.addAgentToParicipant(participant1,agent2)
  zoo.addAgentToParicipant(participant1,agent3)
  zoo.addAgentToParicipant(participant1,agent4)


  zoo.addAgentToParicipant(participant2,agent1)
  zoo.addAgentToParicipant(participant2,agent4)
  zoo.addAgentToParicipant(participant2,agent5)

  zoo.printParticipantAgents()
  println()

  zoo.closeAgent(agent2)
  zoo.closeAgent(agent1)

  zoo.addAgentToParicipant(participant1,agent7)
  zoo.addAgentToParicipant(participant2,agent7)

  zoo.printParticipantAgents()

  zoo.close()
}
