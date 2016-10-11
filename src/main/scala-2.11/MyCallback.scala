import com.twitter.common.zookeeper.DistributedLockImpl
import org.apache.zookeeper.{WatchedEvent, Watcher}

/**
  * Created by revenskiy_ag on 11.10.16.
  */
class MyCallback(zoo: ZooKeeper, path: String, children: Seq[DataNode], masterNode: MasterNode) extends Runnable {
  override def run(): Unit = {
    processChooseOfAgentOfMaster()
  }

  def processChooseOfAgentOfMaster(): Unit = {
    val master = Data.serialize(masterNode.getData.toByteArray)
    children foreach { child =>
      val lockedMaster = new DistributedLockImpl(zoo.zooKeeperClient, s"${masterNode.toString}")
      lockedMaster.lock()

      val agent = child.getData.asInstanceOf[Agent]

      if (agent.masterAgents.isDefinedAt(masterNode.toString)) {
        if (agent.masterAgents(masterNode.toString) == master) {
          chooseAgentOfMaster(children)
        } else {
          agent.masterAgents(masterNode.toString) = agent
        }
      }
      else agent.masterAgents += ((masterNode.toString, agent))
      child.setData(agent)

      lockedMaster.unlock()
    }
  }

  private def chooseAgentOfMaster(agents: Seq[DataNode]) : Unit = {
    val agentToBeMaster = agents(scala.util.Random.nextInt(agents.length))
    masterNode.setData(agentToBeMaster.getData)
  }
}
