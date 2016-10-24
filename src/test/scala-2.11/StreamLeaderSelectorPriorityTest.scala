import org.scalatest.{FlatSpec, Matchers}


class StreamLeaderSelectorPriorityTest extends FlatSpec with Matchers {

  "An agent" should "be added to a partition of a stream and take the leadership when the partition doesn't contain agents at all" in {
    val zookeeperConnectionString = "172.17.0.2:2181"
    val zoo = new StreamLeaderSelectorPriority(zookeeperConnectionString, "/participant")
    val partition = zoo.addPartition()

    val agent = Agent("192.168.0.1","1111","1", Agent.Priority.Low)

    zoo.addAgentToPartition(partition,agent)

    zoo.getPartitionLeaderOpt(partition) shouldBe Some(agent)
    zoo.close()
  }

  it should "be deleted and a partition shouldn't contain a leader" in {
    val zookeeperConnectionString = "172.17.0.2:2181"
    val zoo = new StreamLeaderSelectorPriority(zookeeperConnectionString, "/participant")
    val partition = zoo.addPartition()

    val agent = Agent("192.168.0.1","1111","1", Agent.Priority.Low)

    zoo.addAgentToPartition(partition,agent)

    zoo.closeAgent(agent)

    zoo.getPartitionLeaderOpt(partition) shouldBe None
    zoo.close()
  }

  it should "not fail if we try to delete agent that doesn't exits in partition" in {
    val zookeeperConnectionString = "172.17.0.2:2181"
    val zoo = new StreamLeaderSelectorPriority(zookeeperConnectionString, "/participant")
    val partition = zoo.addPartition()

    val agent = Agent("192.168.0.1","1111","1", Agent.Priority.Low)

    zoo.closeAgent(agent)

    zoo.getPartitionLeaderOpt(partition) shouldBe None
    zoo.close()
  }

  "An agent with normal priority" should "be added to a partition of a stream and take the leadership when the partition contain agents with low priority" in {
    val zookeeperConnectionString = "172.17.0.2:2181"
    val zoo = new StreamLeaderSelectorPriority(zookeeperConnectionString, "/participant")
    val partition = zoo.addPartition()

    val agent1 = Agent("192.168.0.1","1111","1", Agent.Priority.Low)
    val agent2 = Agent("192.168.0.2","2222","1", Agent.Priority.Low)
    val agent3 = Agent("192.168.0.3","3333","1", Agent.Priority.Low)
    val agent4 = Agent("192.168.0.4","4444","1", Agent.Priority.Low)

    zoo.addAgentToPartition(partition,agent1)
    zoo.addAgentToPartition(partition,agent2)
    zoo.addAgentToPartition(partition,agent3)
    zoo.addAgentToPartition(partition,agent4)


    val agent5 = Agent("192.168.0.5","5555","1", Agent.Priority.Normal)
    zoo.addAgentToPartition(partition,agent5)


    zoo.getPartitionLeaderOpt(partition) shouldBe Some(agent5)
    zoo.close()
  }

  it should "be added to a partition of a stream when the partition contain agents with normal priority" in {
    val zookeeperConnectionString = "172.17.0.2:2181"
    val zoo = new StreamLeaderSelectorPriority(zookeeperConnectionString, "/participant")
    val partition = zoo.addPartition()

    val agent1 = Agent("192.168.0.1","1111","1", Agent.Priority.Normal)
    val agent2 = Agent("192.168.0.2","2222","1", Agent.Priority.Normal)
    val agent3 = Agent("192.168.0.3","3333","1", Agent.Priority.Normal)
    val agent4 = Agent("192.168.0.4","4444","1", Agent.Priority.Normal)
    val agent5 = Agent("192.168.0.5","5555","1", Agent.Priority.Normal)

    zoo.addAgentToPartition(partition,agent1)
    zoo.addAgentToPartition(partition,agent2)
    zoo.addAgentToPartition(partition,agent3)
    zoo.addAgentToPartition(partition,agent4)
    zoo.addAgentToPartition(partition,agent5)

    zoo.getPartitionLeaderOpt(partition) shouldBe Some(agent1)

    zoo.close()
  }

  it should "be deleted and let be a leader among with normal priority" in {
    val zookeeperConnectionString = "172.17.0.2:2181"
    val zoo = new StreamLeaderSelectorPriority(zookeeperConnectionString, "/participant")
    val partition = zoo.addPartition()

    val agent1 = Agent("192.168.0.1","1111","1", Agent.Priority.Normal)
    val agent2 = Agent("192.168.0.2","2222","1", Agent.Priority.Normal)
    val agent3 = Agent("192.168.0.3","3333","1", Agent.Priority.Normal)
    val agent4 = Agent("192.168.0.4","4444","1", Agent.Priority.Normal)
    val agent5 = Agent("192.168.0.5","5555","1", Agent.Priority.Normal)

    zoo.addAgentToPartition(partition,agent1)
    zoo.addAgentToPartition(partition,agent2)
    zoo.addAgentToPartition(partition,agent3)
    zoo.addAgentToPartition(partition,agent4)
    zoo.addAgentToPartition(partition,agent5)

    zoo.closeAgent(agent1)
    val agentsOpt = Some(agent2) :: Some(agent3) :: Some(agent4) :: Some(agent5) :: Nil

    agentsOpt should contain (zoo.getPartitionLeaderOpt(partition))

    zoo.close()
  }

  it should "be deleted and let be a leader among agents with low priority" in {
    val zookeeperConnectionString = "172.17.0.2:2181"
    val zoo = new StreamLeaderSelectorPriority(zookeeperConnectionString, "/participant")
    val partition = zoo.addPartition()

    val agent1 = Agent("192.168.0.1","1111","1", Agent.Priority.Low)
    val agent2 = Agent("192.168.0.2","2222","1", Agent.Priority.Low)
    val agent3 = Agent("192.168.0.3","3333","1", Agent.Priority.Low)
    val agent4 = Agent("192.168.0.4","4444","1", Agent.Priority.Low)

    zoo.addAgentToPartition(partition,agent1)
    zoo.addAgentToPartition(partition,agent2)
    zoo.addAgentToPartition(partition,agent3)
    zoo.addAgentToPartition(partition,agent4)

    val agent5 = Agent("192.168.0.5","5555","1", Agent.Priority.Normal)
    zoo.addAgentToPartition(partition, agent5)

    zoo.closeAgent(agent5)
    val agentsOpt = Some(agent1) :: Some(agent2) :: Some(agent3) :: Some(agent4) :: Nil

    agentsOpt should contain (zoo.getPartitionLeaderOpt(partition))

    zoo.close()
  }




  "An agent with low priority" should "be added to a partition of a stream when the partition contain agents with low priority" in {
    val zookeeperConnectionString = "172.17.0.2:2181"
    val zoo = new StreamLeaderSelectorPriority(zookeeperConnectionString, "/participant")
    val partition = zoo.addPartition()

    val agent1 = Agent("192.168.0.1","1111","1", Agent.Priority.Low)
    val agent2 = Agent("192.168.0.2","2222","1", Agent.Priority.Low)
    val agent3 = Agent("192.168.0.3","3333","1", Agent.Priority.Low)
    val agent4 = Agent("192.168.0.4","4444","1", Agent.Priority.Low)

    zoo.addAgentToPartition(partition,agent1)
    zoo.addAgentToPartition(partition,agent2)
    zoo.addAgentToPartition(partition,agent3)
    zoo.addAgentToPartition(partition,agent4)

    val agent5 = Agent("192.168.0.5","5555","1", Agent.Priority.Low)
    zoo.addAgentToPartition(partition,agent5)

    zoo.getPartitionLeaderOpt(partition) shouldBe Some(agent1)

    zoo.close()
  }

  it should "be added to a partition of a stream when the partition contain agents with normal priority" in {
    val zookeeperConnectionString = "172.17.0.2:2181"
    val zoo = new StreamLeaderSelectorPriority(zookeeperConnectionString, "/participant")
    val partition = zoo.addPartition()

    val agent1 = Agent("192.168.0.1","1111","1", Agent.Priority.Normal)
    val agent2 = Agent("192.168.0.2","2222","1", Agent.Priority.Normal)
    val agent3 = Agent("192.168.0.3","3333","1", Agent.Priority.Normal)
    val agent4 = Agent("192.168.0.4","4444","1", Agent.Priority.Normal)

    zoo.addAgentToPartition(partition,agent1)
    zoo.addAgentToPartition(partition,agent2)
    zoo.addAgentToPartition(partition,agent3)
    zoo.addAgentToPartition(partition,agent4)

    val agent5 = Agent("192.168.0.5","5555","1", Agent.Priority.Low)
    zoo.addAgentToPartition(partition,agent5)

    zoo.getPartitionLeaderOpt(partition) shouldBe Some(agent1)

    zoo.close()
  }

  it should "be deleted and let be a leader among with normal priority" in {
    val zookeeperConnectionString = "172.17.0.2:2181"
    val zoo = new StreamLeaderSelectorPriority(zookeeperConnectionString, "/participant")
    val partition = zoo.addPartition()

    val agent1 = Agent("192.168.0.1","1111","1", Agent.Priority.Normal)
    val agent2 = Agent("192.168.0.2","2222","1", Agent.Priority.Normal)
    val agent3 = Agent("192.168.0.3","3333","1", Agent.Priority.Normal)
    val agent4 = Agent("192.168.0.4","4444","1", Agent.Priority.Normal)

    zoo.addAgentToPartition(partition,agent1)
    zoo.addAgentToPartition(partition,agent2)
    zoo.addAgentToPartition(partition,agent3)
    zoo.addAgentToPartition(partition,agent4)

    val agent5 = Agent("192.168.0.5","5555","1", Agent.Priority.Low)
    zoo.addAgentToPartition(partition,agent5)
    zoo.closeAgent(agent5)


    zoo.getPartitionLeaderOpt(partition) shouldBe Some(agent1)

    zoo.close()
  }

  it should "be deleted and let be a leader among with low priority" in {
    val zookeeperConnectionString = "172.17.0.2:2181"
    val zoo = new StreamLeaderSelectorPriority(zookeeperConnectionString, "/participant")
    val partition = zoo.addPartition()

    val agent1 = Agent("192.168.0.1","1111","1", Agent.Priority.Low)
    val agent2 = Agent("192.168.0.2","2222","1", Agent.Priority.Low)
    val agent3 = Agent("192.168.0.3","3333","1", Agent.Priority.Low)
    val agent4 = Agent("192.168.0.4","4444","1", Agent.Priority.Low)
    val agent5 = Agent("192.168.0.5","5555","1", Agent.Priority.Low)

    zoo.addAgentToPartition(partition,agent1)
    zoo.addAgentToPartition(partition,agent2)
    zoo.addAgentToPartition(partition,agent3)
    zoo.addAgentToPartition(partition,agent4)
    zoo.addAgentToPartition(partition,agent5)


    zoo.closeAgent(agent1)

    val agentsOpt = Some(agent2) :: Some(agent3) :: Some(agent4) :: Some(agent5) :: Nil

    agentsOpt should contain (zoo.getPartitionLeaderOpt(partition))

    zoo.close()
  }

}
