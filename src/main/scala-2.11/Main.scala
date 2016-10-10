
import java.net.{InetAddress, InetSocketAddress}

import com.twitter.common.zookeeper.{ZooKeeperClient, ZooKeeperNode, ZooKeeperUtils}
import org.apache.zookeeper.{CreateMode, ZooDefs}
import org.apache.zookeeper


object Main extends App {

  val ipAddress = InetAddress.getByName("172.17.0.2")
  val port = 2181

  val zooKeeper = new ZooKeeper(ipAddress, port)

 // zooKeeper.zooKeeper.getChildren("/",false)

  val root = Root(zooKeeper, "master","participant")
  root.masterPath
  root.participantPath

  val master = MasterNode(zooKeeper,root.masterPath.toString)

  root.masterPath.children += master
  root.masterPath.children += MasterNode(zooKeeper,root.masterPath.toString)

  //root.masterPath.children += MasterNode(root.zoo,root.masterPath.name)

  val agentAnother = Agent("172.16.0.1","2121","3")
  master.setData(agentAnother)
  println(master.getData)



  println(root.masterPath.children)

//  val stream = new Stream(zooKeeper,"participant","master")
////    stream.configure()
//
//  val agent = Agent("192.168.0.1","2222","1")
//  val agentAnother = Agent("172.16.0.1","2121","3")
//
//
//  val  (masterId,partcipantId) = stream.createMasterIdAndPaticipantIdAndBindThem(Some(agent))
//  stream.addAgentToParticipant(partcipantId,Agent("192.168.0.2","2552","2"))
//  stream.addAgentToParticipant(partcipantId,Agent("192.168.0.3","2662","3"))
//  stream.addAgentToParticipant(partcipantId,Agent("192.168.0.4","2662","4"))
//  stream.addAgentToParticipant(partcipantId,Agent("192.168.0.5","2662","5"))
//  stream.addAgentToParticipant(partcipantId,Agent("192.168.0.6","2662","6"))
//  stream.addAgentToParticipant(partcipantId,Agent("192.168.0.7","2662","7"))
//
// // val master = stream.getMasterData()
//  println(agent.masterAgents)





//  stream.createMasterIdAndPaticipantIdAndBindThem(Some(agent))
//  stream.createMasterIdAndPaticipantIdAndBindThem(Some(agent))
//  stream.createMasterIdAndPaticipantIdAndBindThem(Some(agent))
//  stream.createMasterIdAndPaticipantIdAndBindThem(Some(agent))


}
