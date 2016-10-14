import org.scalatest.{FlatSpec, Matchers}

import scala.util.Random

/**
  * Created by revenskiy_ag on 14.10.16.
  */
class ElectionIteartionTest extends FlatSpec with Matchers {

  "A election process" should "be stable to n-iterations for 2 partitions" in {
    val zookeeperConnectionString = "172.17.0.2:2181"
    val zoo = new ZooTree(zookeeperConnectionString, "/participant")
    val patrition1 = zoo.addPartition()
    val patrition2 = zoo.addPartition()

    val iterationNumber = 50
    (1 to iterationNumber).foreach{_=>
      val agent = Agent(Random.nextInt(10).toString, Random.nextInt(4).toString, Random.nextInt(2).toString)

      val bool = scala.util.Random.nextBoolean()
      if (bool) zoo.addAgentToPartrition(patrition1,agent) else zoo.addAgentToPartrition(patrition2,agent)

      zoo.isAllPatritionsAgentsHaveTheSameLeader shouldBe true
    }
    zoo.close()
  }

  it should "be stable to n-iterations for 2 partitions and closing of agents" in {
    val zookeeperConnectionString = "172.17.0.2:2181"
    val zoo = new ZooTree(zookeeperConnectionString, "/participant")
    val patrition1 = zoo.addPartition()
    val patrition2 = zoo.addPartition()

    val iterationNumber = 50
    val agents = (1 to iterationNumber).toList.map { _ =>
      val agent = Agent(Random.nextInt(10000).toString, Random.nextInt(4000).toString, Random.nextInt(2).toString)

      val bool = scala.util.Random.nextBoolean()
      if (bool) zoo.addAgentToPartrition(patrition1, agent) else zoo.addAgentToPartrition(patrition2, agent)

      zoo.isAllPatritionsAgentsHaveTheSameLeader shouldBe true
      agent
    }

    util.Random.shuffle(agents) foreach {agent =>
      zoo.closeAgent(agent)
      zoo.isAllPatritionsAgentsHaveTheSameLeader shouldBe true
    }
    zoo.close()
  }

}
