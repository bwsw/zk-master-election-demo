import LeaderSelectorPriority.Priority
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.ExponentialBackoffRetry
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.mutable.ArrayBuffer

class LeaderSelectorPriorityTest extends FlatSpec with Matchers {

  "All leader selectors" should "have the same leader" in {

    val CLIENT_QTY: Int = 20
    val PATH: String = "/examples/leader0"

    val stream = new LeaderSelectorPriority(PATH)
    val connectionString = "172.17.0.2:2181"

    val clients: ArrayBuffer[CuratorFramework] = ArrayBuffer()
    (1 to CLIENT_QTY) foreach { i =>
      val client = CuratorFrameworkFactory.newClient(connectionString, new ExponentialBackoffRetry(100, 2))
      clients += client
      client.start()
      stream.addId("Client #" + i, Priority.Low, client)
    }

    val client = CuratorFrameworkFactory.newClient(connectionString, new ExponentialBackoffRetry(100, 2))
    clients += client
    client.start()
    stream.addId("Client #" + CLIENT_QTY + 1, Priority.Normal, client)

    stream.isAllLeaderSelectorsHaveTheSameLeader shouldBe true

    clients foreach (_.close())
  }
}
