import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.ExponentialBackoffRetry
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.mutable.ArrayBuffer

class MyLeaderSelectorClientTest extends FlatSpec with Matchers{
  "Leader Selector" should "take a leadership predictably" in {
    val CLIENT_QTY: Int = 1
    val PATH: String = "/examples/leader4"

    val clients: ArrayBuffer[CuratorFramework] = ArrayBuffer()
    val examples: ArrayBuffer[MyLeaderSelectorClient] = ArrayBuffer()
    val connectionString = "172.17.0.2:2181"
    try {
      (0 to CLIENT_QTY) foreach { i =>
        val client = CuratorFrameworkFactory.newClient(connectionString, new ExponentialBackoffRetry(100, 2))
        val example = new MyLeaderSelectorClient(client, PATH, "Client #" + i)

        clients += client
        examples += example

        client.start()
        example.start()
      }
    }


    val currentLeader = examples.head.getLeader.getId

    val exampleOldLeaderIndex = examples.indexWhere(_.id == currentLeader)
    val exampleNewLeaderIndex = if (exampleOldLeaderIndex == 1) 0 else 1

    examples.map(_.getLeader).distinct.length shouldBe 1

    examples(exampleOldLeaderIndex).release()

    examples.map(_.getLeader).distinct.length shouldBe 1

    examples(exampleNewLeaderIndex).release()
    examples(exampleOldLeaderIndex).requeue()



    examples.map(_.getLeader).distinct.length shouldBe 1

    clients foreach (_.close())
  }

}
