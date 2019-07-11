import java.util

import com.aliyun.odps.{Instance, Odps}
import com.aliyun.odps.account.AliyunAccount
import com.aliyun.odps.data.{Record, ResultSet}
import com.aliyun.odps.task.SQLTask

/**
  * title: ReadFromOdps
  * projectName user-label
  * description: 使用SQLAPI从odps表中读取数据，一次只能读取1万条记录
  * author QianNan
  * date 2019/7/11
  */
object ReadFromOdpsWithSQL {

  def main(args: Array[String]): Unit = {

    val account = new AliyunAccount("LTAI0L2uSqErjaNj", "OmI670FSszHd9zcElsigEYwddLHiEZ")
    val odps = new Odps(account)
    odps.setEndpoint("http://service.odps.aliyun.com/api")
    odps.setDefaultProject("kaiqi_k7game_t")

    val instance: Instance = SQLTask.run(odps, "select * from dw_user;")
    instance.waitForSuccess()
 //   val resultSet: ResultSet = SQLTask.getResultSet(instance)
    val records: util.List[Record] = SQLTask.getResult(instance)
    import scala.collection.JavaConversions._
    for (r <- records) {
      System.out.println(r.get(4).toString)
    }
//    while (resultSet.hasNext){
//
//      val record: Record = resultSet.next()
//      if(record.get("register_spm") != null){
//
//        println(record.get("user_id"))
//      }
//
//    }
  }

}
