import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.framework.recipes.leader.LeaderLatch
import org.apache.curator.retry.ExponentialBackoffRetry

/**
  * Created by revenskiy_ag on 13.10.16.
  */
class LeaderLatchExample(client: CuratorFramework, latchPath: String, id: String) {
  private val leaderLatch = new LeaderLatch(client, latchPath, id)

  def start() = {
    leaderLatch.start()
  }

  def isLeader = {
    leaderLatch.hasLeadership
  }

  def currentLeader = {
    leaderLatch.getLeader
  }

  def name = {
    leaderLatch.getId
  }

  def close = {
    leaderLatch.close()
  }
}
