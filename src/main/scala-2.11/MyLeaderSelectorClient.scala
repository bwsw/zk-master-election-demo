import java.io.{Closeable, IOException}
import java.util.concurrent.{ExecutorService, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.leader.{LeaderLatchListener, LeaderSelector, LeaderSelectorListenerAdapter, Participant}

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}


// create a leader selector using the given path for management
// all participants in a given leader selection must use the same path
// ExampleClient here is also a LeaderSelectorListener but this isn't required
class MyLeaderSelectorClient(client: CuratorFramework, path: String,val id: String) extends
  LeaderSelectorListenerAdapter with Closeable {
  private val leaderSelector: LeaderSelector =  new LeaderSelector(client, path, this)
  private val resettableCountDownLatch = new ResettableCountDownLatch(1)
  private var isNotStarted = true

  leaderSelector.setId(id)

  val leaderCount: AtomicInteger = new AtomicInteger

  @throws[IOException]
  def start() = {
    // the selection for this instance doesn't start until the leader selector is started
    // leader selection is done in the background so this call to leaderSelector.start() returns immediately
    if (isNotStarted) {leaderSelector.start(); isNotStarted = false} else leaderSelector.requeue()
  }
  def requeue() = Thread.sleep(1); start()

  def isStarted = !isNotStarted

  @tailrec
  final def getLeader: Participant = {
    import org.apache.zookeeper.KeeperException.NoNodeException

    val timeToSleep = 10L
    val tryGetLeader = scala.util.Try(leaderSelector.getLeader)
    tryGetLeader match {
      case Success(leader) => if (leader.getId == "") {Thread.sleep(timeToSleep); getLeader} else leader
      case Failure(error) => error match {
        case noNode: NoNodeException => {
          Thread.sleep(timeToSleep); getLeader
        }
        case _ => throw error
      }
    }
  }

  def release() = {
    //Thread sleep makes a differ ,otherwise it shouldn't work properly
    val timeToSleep = 30L
    resettableCountDownLatch.countDown()
    Thread.sleep(timeToSleep)
    resettableCountDownLatch.reset
  }

  @throws[IOException]
  def close() {
    scala.util.Try(leaderSelector.close()) match {
      case Success(_) => ()
      case Failure(error) => println("Need to update zookeeper client version to 2.11.1")
    }
  }

  @throws[Exception]
  def takeLeadership(client: CuratorFramework) {resettableCountDownLatch.await()}
}

object MyLeaderSelectorClient {
  //val resettableCountDownLatch = new ResettableCountDownLatch(1)
}
