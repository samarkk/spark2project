package datagen

import java.io.{File, PrintWriter}
import scala.util.Random

case class Mobile(id: Int, name: String)

case class MobileSpecs(mobileid: Int, modelId: String, cores: Int, memory: Int, storage: Int)

case class MobileSales(mobileid: Int, modelId: String, state: String, city: String, units: Int)

object Generator {
  val mobileBrands: Seq[Mobile] = Seq("Apple", "Sony", "Lava", "Intex", "Micromax", "Nokia", "Google", "Sharp", "Karbon", "Samsung").zipWithIndex.map(x => Mobile(x._2, x._1))
  val cores = Seq(2, 4, 6, 8)
  val memory = Seq(1, 2, 3, 4, 6, 8, 12)
  val storage = Seq(16, 32, 64, 128, 256, 512, 1024)

  def generateMobileModels(n: Int, numModels: Int): Seq[MobileSpecs] = for (x <- 1 to n) yield {
    val rndm = new Random()
    val mobileId = rndm.nextInt(mobileBrands.length)
    val modelId = mobileBrands(mobileId).name.take(2) + rndm.nextInt(numModels)
    val mobileCores = cores(rndm.nextInt(cores.size))
    val mobileMemory = memory(rndm.nextInt(memory.size))
    val mobileStorage = storage(rndm.nextInt(storage.size))
    MobileSpecs(mobileId, modelId, mobileCores, mobileMemory, mobileStorage)
  }

  def generateMobileSales(n: Int, numModels: Int, uniform: Boolean = true): Seq[MobileSales] = for (i <- 1 to n)
    yield {
      val rndm = new Random()
      val mobileId = if (uniform) rndm.nextInt(mobileBrands.length) else {
        if (rndm.nextInt(10) % 2 == 0)
          rndm.nextInt(2)
        else rndm.nextInt(10)
      }
      val modelId = mobileBrands(mobileId).name.take(2) + rndm.nextInt(numModels)
      val state = "State" + (1 + rndm.nextInt(29))
      val city = "City" + (1 + rndm.nextInt(100))
      val units = rndm.nextInt(10)
      MobileSales(mobileId, modelId, state, city, units)
    }

  val random = new Random()

  def randomString(n: Int) = new String((
    for (x <- 1 to n)
      yield ('a' + random.nextInt(26)).toChar)
    .toArray)

  def generatePlayerNames(n: Int): Seq[String] = for (_ <- 1 to n) yield randomString(10)

  def generateIds(n: Int): Seq[Int] = 1 to n

  def generateIdsAndNames(n: Int): Seq[String] = for (x <- generateIds(n)) yield {
    x + "," + randomString(10)
  }

  def generateIdsAndScores(n: Int): Seq[String] = generateIds(n).flatMap(x => {
    Seq.fill(5 + random.nextInt(5))(x + "," + random.nextInt(10000))
  })

  val emailSuffixes: Array[String] = Array("com", "us", "edu", "net", "ind")

  def generatePlayerEmails(playersFile: String): Iterator[String] = {
    val playersList = scala.io.Source.fromFile(new File(playersFile))
    playersList.getLines.toIterator.map(x =>
      x.split(",")(0) + "," +
        x.split(",")(1) + "@" + randomString(5) + "." +
        emailSuffixes(random.nextInt(emailSuffixes.length)))
  }

  //  def generatePlayerEmails(n: I): Seq[String] = generatePlayerNames(n).map(x => x + "@" + randomString(5) + "." + emailSuffixes(random.nextInt(emailSuffixes.length)))

  def writeRDDsToFile(n: Int, baseDir: String): Unit = {
    val namesRDDFile = new File(baseDir + "/ids_and_names.txt")

    val namesWriter = new PrintWriter(namesRDDFile)
    generateIdsAndNames(n).foreach(namesWriter.println)
    namesWriter.flush()
    namesWriter.close()

    val scoreswFile = new File(baseDir + "/ids_and_scores.txt")
    val scoresWriter = new PrintWriter(scoreswFile)
    generateIdsAndScores(n).foreach(scoresWriter.println)
    //    scoresWriter.flush()
    scoresWriter.close()

    val emailsFile = new File(baseDir + "/ids_and_emails.txt")
    val emailsWriter = new PrintWriter(emailsFile)
    generatePlayerEmails(baseDir + "/ids_and_names.txt").foreach(x => emailsWriter.println(x))
    emailsWriter.flush()
    emailsWriter.close()
  }
}

object GenData extends App {
  Generator.writeRDDsToFile(100, "D:/ufdata/plrdata")
}