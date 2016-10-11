
import java.net.InetAddress


object Main extends App {


  val ipAddress = InetAddress.getByName("172.17.0.2")
  val port = 2181

  val zooKeeper = new ZooKeeper(ipAddress, port)

  zooKeeper.zooKeeper.getChildren("/",false)

  val root = Root(zooKeeper, "participant", "master")


  val master1 = MasterNode(zooKeeper,root.masterPath)
  val master2 = MasterNode(zooKeeper,root.masterPath)
  val master3 = MasterNode(zooKeeper,root.masterPath)
  val master4 = MasterNode(zooKeeper,root.masterPath)
  val master5 = MasterNode(zooKeeper,root.masterPath)
  val master6 = MasterNode(zooKeeper,root.masterPath)
  root.masterPath.children += master1
  root.masterPath.children += master2
  root.masterPath.children += master3
  root.masterPath.children += master4
  root.masterPath.children += master5
  root.masterPath.children += master6

  val participant = ParticipantNode(zooKeeper,root.participantPath, master1)
  root.participantPath.children += participant


  val agent1 = Agent("192.168.0.1","2222","1"); val dataNode1 = DataNode(zooKeeper,participant, agent1)
  val agent2 = Agent("192.168.0.2","2222","1"); val dataNode2 = DataNode(zooKeeper,participant, agent2)
  val agent3 = Agent("192.168.0.3","2222","1"); val dataNode3 = DataNode(zooKeeper,participant, agent3)
  val agent4 = Agent("192.168.0.4","2222","1"); val dataNode4 = DataNode(zooKeeper,participant, agent4)
  val agent5 = Agent("192.168.0.5","2222","1"); val dataNode5 = DataNode(zooKeeper,participant, agent5)
  val agent6 = Agent("192.168.0.6","2222","1"); val dataNode6 = DataNode(zooKeeper,participant, agent6)

  participant.addChildAndCreate(dataNode1)
  participant.addChildAndCreate(dataNode2)
  participant.addChildAndCreate(dataNode3)
  participant.addChildAndCreate(dataNode4)
  participant.addChildAndCreate(dataNode5)
  participant.addChildAndCreate(dataNode6)

}
