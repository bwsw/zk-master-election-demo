import java.io.{Closeable, IOException}
import java.util.concurrent.{ExecutorService, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.leader.{LeaderLatchListener, LeaderSelector, LeaderSelectorListenerAdapter}


// create a leader selector using the given path for management
// all participants in a given leader selection must use the same path
// ExampleClient here is also a LeaderSelectorListener but this isn't required
class MyLeaderSelectorClient(client: CuratorFramework, path: String, name: String, exec: ExecutorService) extends
  LeaderSelectorListenerAdapter with Closeable {
  val leaderSelector: LeaderSelector =  new LeaderSelector(client, path, exec, this)
  val resettableCountDownLatch = new ResettableCountDownLatch(1)

  leaderSelector.setId(name)

  //\\leaderSelector.autoRequeue()
  val leaderCount: AtomicInteger = new AtomicInteger

  @throws[IOException]
  def start() {
    // the selection for this instance doesn't start until the leader selector is started
    // leader selection is done in the background so this call to leaderSelector.start() returns immediately
    leaderSelector.start()
  }

  def reset() = resettableCountDownLatch.reset

  def release() = resettableCountDownLatch.countDown()

  @throws[IOException]
  def close() {
    leaderSelector.close()
  }

  @throws[Exception]
  def takeLeadership(client: CuratorFramework) {
    System.out.println(name + " has been leader " + leaderCount.getAndIncrement + " time(s) before.")
    resettableCountDownLatch.await()

    // we are now the leader. This method should not return until we want to relinquish leadership
   // val waitSeconds: Int = (5 * Math.random).toInt + 1
   // System.out.println(name + " " + leaderSelector.getParticipants)
 //   System.out.println(name + " is now the leader. Waiting " + waitSeconds + " seconds...")
//      try {
//        Thread.sleep(TimeUnit.SECONDS.toMillis(waitSeconds))
//      }
//      catch {
//        case e: InterruptedException => {
//          System.out.println(name + " was interrupted.")
//          Thread.currentThread.interrupt()
//        }
//      }
//       finally  println(name + " relinquishing leadership.\n")
    }
}

object MyLeaderSelectorClient {
  //val resettableCountDownLatch = new ResettableCountDownLatch(1)
}
