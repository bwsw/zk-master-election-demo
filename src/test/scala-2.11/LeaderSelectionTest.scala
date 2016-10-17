import org.scalatest.{FlatSpec, Matchers}

import scala.util.Random

/**
  * Created by revenskiy_ag on 13.10.16.
  */
class LeaderSelectionTest extends FlatSpec with Matchers {

  "A leader/master" should "be selected in such way, that agents in a partition have the same leader" in {
    val zookeeperConnectionString = "172.17.0.2:2181"
    val zoo = new Stream(zookeeperConnectionString, "/participant")

    val partition1 = zoo.addPartition()
    val partition2 = zoo.addPartition()


    val agent1 = Agent("192.168.0.1","1111","1")
    val agent2 = Agent("192.168.0.2","2222","1")
    val agent3 = Agent("192.168.0.3","3333","1")
    val agent4 = Agent("192.168.0.4","4444","1")
    val agent5 = Agent("192.168.0.5","5555","1")
    val agent6 = Agent("192.168.0.6","6666","1")
    val agent7 = Agent("192.168.0.7","7777","1")

    zoo.addAgentToPartrition(partition1,agent1)
    zoo.addAgentToPartrition(partition1,agent2)
    zoo.addAgentToPartrition(partition1,agent3)
    zoo.addAgentToPartrition(partition1,agent4)


    zoo.addAgentToPartrition(partition2,agent1)
    zoo.addAgentToPartrition(partition2,agent4)
    zoo.addAgentToPartrition(partition2,agent5)
    zoo.addAgentToPartrition(partition1,agent7)
    zoo.addAgentToPartrition(partition2,agent7)

    zoo.close()
    zoo.isAllPatritionsAgentsHaveTheSameLeader shouldBe true
  }
}
