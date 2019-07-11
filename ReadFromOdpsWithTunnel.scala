import com.aliyun.odps.{Odps, PartitionSpec}
import com.aliyun.odps.account.AliyunAccount
import com.aliyun.odps.data.Record
import com.aliyun.odps.tunnel.TableTunnel
import com.aliyun.odps.tunnel.io.TunnelRecordReader

/**
  * title: ReadFromOdpsWithTunnel
  * projectName user-label
  * description: TODO
  * author QianNan
  * date 2019/7/11
  */
object ReadFromOdpsWithTunnel {

  def main(args: Array[String]): Unit = {

    val account = new AliyunAccount("LTAI0L2uSqErjaNj", "OmI670FSszHd9zcElsigEYwddLHiEZ")

    val odps = new Odps(account)
    odps.setEndpoint("http://service.odps.aliyun.com/api")
    odps.setDefaultProject("kaiqi_k7game_t")

    val tunnel = new TableTunnel(odps)
    val spec = new PartitionSpec("year_id=2015")
    tunnel.setEndpoint("http://dt.odps.aliyun.com")

    val session: TableTunnel#DownloadSession = tunnel.createDownloadSession("kaiqi_k7game_t","dw_user",spec)
    val count: Long = session.getRecordCount

    println(count)
    val tunnelReader: TunnelRecordReader = session.openRecordReader(0,count)
    var i = 1
    while (i<=count){

      val record: Record = tunnelReader.read()
      if(record.get("register_spm")!=null){

        println(record.getString("register_spm"))
      }
      i= i+1
    }

    tunnelReader.close()

  }

}
