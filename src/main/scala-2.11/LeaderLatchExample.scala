import java.util.concurrent.TimeUnit

import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.leader.{LeaderLatch, LeaderLatchListener}

class LeaderLatchExample(client: CuratorFramework, latchPath: String, id: String) {
  private val leaderLatch = new LeaderLatch(client, latchPath, id)

  def listener = new LeaderLatchListener{
    def isLeader = {
      leaderLatch.close()
    }
    def notLeader(): Unit = {
      leaderLatch.await()
    }
  }

  def start() = {
    leaderLatch.start()
    leaderLatch.addListener(listener)
  }

  def getId = leaderLatch.getId

  def getLeader = leaderLatch.getLeader

  def isLeader = {
    println("asdsad")
  }

  def currentLeader() = {
    leaderLatch.getLeader
  }

  def close() = {
    leaderLatch.close(LeaderLatch.CloseMode.NOTIFY_LEADER)
  }

  def notLeader(): Unit = {
    println("asdsad")
  }

}