import model.{Operations, Log}

object Application extends App {
  try {
    val operator = new Operations()
    val result = operator.getTotalSales
    operator.writeResult(result)
    operator.sc.stop()
  } catch {
    case ex: Exception => Log.info(s"\n ERROR: ${ex.getCause}");
  }
}
