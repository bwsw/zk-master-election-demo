import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.framework.recipes.leader.LeaderLatch
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.curator.{RetryPolicy, RetrySleeper}
import org.apache.zookeeper.CreateMode

/**
  * Created by revenskiy_ag on 12.10.16.
  */
object Main extends App{
  val zookeeperConnectionString = "172.17.0.2:2181"
  val policy = new ExponentialBackoffRetry(1000,30)
  val client = CuratorFrameworkFactory.newClient(zookeeperConnectionString,policy)
  client.start()
  client.getZookeeperClient.blockUntilConnectedOrTimedOut()
  val zoo = new ZooTree(client,"/master","/participant")
  val master1 = zoo.addMaster
  val master2 = zoo.addMaster

  val participant1 = zoo.addParticipant(master1)
  val participant2 = zoo.addParticipant(master2)

  //println(zoo.getParticipantData(participant1))
  //println(zoo.getParticipantData(participant2))

  val agent1 = Agent("192.168.0.1","1111","1")
  val agent2 = Agent("192.168.0.2","2222","1")
  val agent3 = Agent("192.168.0.3","3333","1")
  val agent4 = Agent("192.168.0.4","4444","1")
  val agent5 = Agent("192.168.0.5","5555","1")
  val agent6 = Agent("192.168.0.6","6666","1")

  zoo.addAgentToParicipant(participant1,agent1)
  zoo.addAgentToParicipant(participant1,agent2)
  zoo.addAgentToParicipant(participant1,agent3)
  zoo.addAgentToParicipant(participant1,agent4)
  //zoo.addAgentToParicipant(participant1,agent5)


  val b = zoo.chooseLeaderOfParcipant(participant1)

 // val c = zoo.chooseLeaderOfParcipant(participant1)

  zoo.participants foreach (_.start())

  zoo.addAgentToParicipant(participant2,agent1)
  zoo.addAgentToParicipant(participant2,agent2)
  zoo.addAgentToParicipant(participant2,agent3)

 // zoo.participants foreach (x => if(x.hasLeadership){ x.close();Thread.sleep(300)} else Thread.sleep(300))

  //val d = zoo.chooseLeaderOfParcipant(participant2)

 // println(zoo.participantAgents)




  client.close()
}
