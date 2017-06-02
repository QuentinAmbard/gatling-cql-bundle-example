package rcigroup

import java.sql.Timestamp
import java.time.{LocalDateTime, ZoneOffset}

import com.datastax.driver.core._
import com.datastax.driver.core.policies.{ConstantSpeculativeExecutionPolicy, DCAwareRoundRobinPolicy, TokenAwarePolicy}
import io.gatling.core.Predef._
import io.gatling.core.scenario.Simulation
import io.github.gatling.cql.Predef._

import scala.concurrent.duration.DurationInt
import scala.util.Random


class GatlingSimulationInsertRCI extends Simulation {
  val cluster = Cluster.builder().addContactPoints("localhost")
    //.withCredentials("username", "password")
    .withPoolingOptions(new PoolingOptions().setConnectionsPerHost(HostDistance.LOCAL, 10, 20).setMaxRequestsPerConnection(HostDistance.LOCAL, 3276)
    .setConnectionsPerHost(HostDistance.REMOTE, 10, 20).setMaxRequestsPerConnection(HostDistance.REMOTE, 3276))
    .withQueryOptions(new QueryOptions().setConsistencyLevel(ConsistencyLevel.ONE).setFetchSize(500))
    .withSpeculativeExecutionPolicy(
      new ConstantSpeculativeExecutionPolicy(
        500, // delay before a new execution is launched
        2 // maximum number of executions
      ))
    //Change WithLocalDC("NY") and set the local cluster you want to read/write from
    .withLoadBalancingPolicy(new TokenAwarePolicy(DCAwareRoundRobinPolicy.builder().build())).build()

  val session = cluster.connect()
  //Your C* session
  val cqlConfig = cql.session(session) //Initialize Gatling DSL with your session

  val insertOptin = session.prepare("""insert into ..... values (?, ?) """)
  val insertScnOptin = scenario("insert optin_by_client")
    .feed((Iterator.continually({
      Map("param1" -> Random.alphanumeric.take(10).mkString,
        "param2" -> Timestamp.valueOf(LocalDateTime.now()))
    }))).exec(cql("insert optin_by_client").execute(insertOptin).withParams("${param1}", "${param2}"))

  //test_solr.csv in user-files/data
  val update_UC5 = session.prepare("""select * from test.customer where solr_query= ? limit 10 ;""")
  val readScnUC5 = scenario("test_solr").feed(csv("test_solr.csv")).exec(cql("test_solr").execute(update_UC5).withParams("${solr_query}"))


  val readPerSecPerQuery = 1000
  val searchPerSecPerQuery = 10
  val rampupDurationSec = 100
  val testDurationSec = 100
  setUp(
    insertScnOptin.inject(rampUsersPerSec(1) to 100 during (rampupDurationSec seconds), rampUsersPerSec(100) to 1234 during (testDurationSec seconds)),
    readScnUC5.inject(rampUsersPerSec(1) to searchPerSecPerQuery during (rampupDurationSec seconds), rampUsersPerSec(searchPerSecPerQuery) to searchPerSecPerQuery during (testDurationSec seconds)),
  ).protocols(cqlConfig)
}
