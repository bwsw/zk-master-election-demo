


object Main extends App{
  val zookeeperConnectionString = "172.17.0.2:2181"

  val tree = new ZooTree(zookeeperConnectionString)

  val agent1 = Agent("192.168.0.1","1111","4")
  val agent2 = Agent("192.168.0.2","2222","1")
  val agent3 = Agent("192.168.0.3","3333","1")
  val agent4 = Agent("192.168.0.4","4444","1")
  val agent5 = Agent("192.168.0.5","5555","1")
  val agent6 = Agent("192.168.0.6","6666","1")
  val agent7 = Agent("192.168.0.7","7777","1")


  val stream1 = tree.addStream("stream_1", 3)
  val participants = tree.getStreamPartitions("stream_1")


  val randomParticipants1 = participants
  tree.addAgent(agent1,"stream_1", randomParticipants1)

  val randomParticipants2 = participants
  tree.addAgent(agent2,"stream_1", randomParticipants2)

  tree.addAgent(agent3,"stream_1", randomParticipants2)

  tree.addAgent(agent4,"stream_1", randomParticipants2)

  tree.addAgent(agent5,"stream_1", randomParticipants2)


  stream1.printPatritionAgents()

  tree.close()
}
