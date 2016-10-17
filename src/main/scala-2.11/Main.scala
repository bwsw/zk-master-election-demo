


object Main extends App{
  val zookeeperConnectionString = "172.17.0.2:2181"

  val zoo = new ZooTree(zookeeperConnectionString, "/participant")

  val patrition1 = zoo.addPartition()
  val patrition2 = zoo.addPartition()


  val agent1 = Agent("192.168.0.1","1111","1")
  val agent2 = Agent("192.168.0.2","2222","1")
  val agent3 = Agent("192.168.0.3","3333","1")
  val agent4 = Agent("192.168.0.4","4444","1")
  val agent5 = Agent("192.168.0.5","5555","1")
  val agent6 = Agent("192.168.0.6","6666","1")
  val agent7 = Agent("192.168.0.7","7777","1")

  zoo.addAgentToPartrition(patrition1,agent1)
  zoo.addAgentToPartrition(patrition1,agent2)
  zoo.addAgentToPartrition(patrition1,agent3)
  zoo.addAgentToPartrition(patrition1,agent4)


  zoo.addAgentToPartrition(patrition2,agent1)
  zoo.addAgentToPartrition(patrition2,agent4)
  zoo.addAgentToPartrition(patrition2,agent5)


  zoo.closeAgent(agent2)
  zoo.closeAgent(agent1)

  zoo.addAgentToPartrition(patrition1,agent7)
  zoo.addAgentToPartrition(patrition2,agent7)

  zoo.close()
}
