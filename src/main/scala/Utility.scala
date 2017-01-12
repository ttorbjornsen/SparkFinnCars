package ttorbjornsen.finncars
import java.time.{ZoneId, LocalDate}
import java.time.temporal.ChronoUnit
import java.util.{Calendar, HashMap}

import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.jsoup.Jsoup
import org.jsoup.nodes._
import play.api.libs.json._
import org.apache.spark.sql.functions._

import scala.collection.JavaConversions._
import scala.collection.immutable.Map
import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.util.{Failure, Success, Try}




/**
  * Created by torbjorn.torbjornsen on 04.07.2016.
  */


object Utility {




  def createAcqCarDetailsObject(acqCarHdr:AcqCarHeader)= {
    val jsonCarDetail = scrapeCarDetails(acqCarHdr.url)
    val carProperties = jsonCarDetail("properties").toString
    val carEquipment = jsonCarDetail("equipment").toString
    val carInformation = jsonCarDetail("information").toString
    val deleted = jsonCarDetail("deleted").as[Boolean]
    val load_time = System.currentTimeMillis()
    val load_date = acqCarHdr.load_date //to be able to join acq car header and acq car details on date
    AcqCarDetails(finnkode = acqCarHdr.finnkode, properties = carProperties, equipment = carEquipment, information = carInformation, deleted = deleted, load_time = load_time, load_date = load_date, url=acqCarHdr.url)
  }


    
  def createAcqCarHeaderObjects(jsonCarHdr:JsValue): List[AcqCarHeader] ={
    //val jsonCarHdr:JsValue = Json.parse("""[{"finnkode":"88072242","location":"Larvik","title":"Volvo V40 V40 1,8 aut","year":"2003","km":"197 000 km","price":"26 500,-"},{"finnkode":"76453705","location":"Molde","title":"Saab 9-5 2,0 T Eco Vector aut","year":"2003","km":"205 000 km","price":"64 000,-"},{"finnkode":"85094051","location":"Skien","title":"Volvo V70 2.4 140HK Automat, Delskinn, Cruise++","year":"2003","km":"200 000 km","price":"59 538,-"},{"finnkode":"85049013","location":"Ingeberg","title":"Kia Rio LEVERES MED FERSK EU , 162 000KM","year":"2003","km":"162 000 km","price":"25 000,-"},{"finnkode":"84985526","location":"Sandnes","title":"Skoda Fabia 1.2 HTP 64HK PEN BIL LEVER MD NY EU","year":"2003","km":"232 013 km","price":"29 538,-"},{"finnkode":"84974699","location":"Oslo","title":"Volkswagen Touran 1,9 TDI /hengerfeste","year":"2003","km":"186 000 km","price":"46 538,-"},{"finnkode":"84954517","location":"Ramfjordbotn","title":"Subaru Forester AUTOMAT/4X4/12.MND GARANTI","year":"2003","km":"187 000 km","price":"89 900,-"},{"finnkode":"84953771","location":"Holmestrand","title":"Toyota Avensis","year":"2003","km":"217 500 km","price":"33 538,-"},{"finnkode":"84930978","location":"Harstad","title":"Audi A4 1.9 TDI 2 S-line Quattro, navi","year":"2003","km":"187 000 km","price":"89 500,-"},{"finnkode":"84894142","location":"Ingeberg","title":"Audi A4 2.0 avant. pen bil. eu ok","year":"2003","km":"206 000 km","price":"69 000,-"},{"finnkode":"84700655","location":"Haukeland","title":"Kia Carens Greit familie bil til salgs","year":"2003","km":"138 020 km","price":"27 038,-"},{"finnkode":"84674989","location":"Lierstranda","title":"Toyota Corolla FAMILIEBIL MED GOD PLASS","year":"2003","km":"182 000 km","price":"38 538,-"},{"finnkode":"84670947","location":"Asker","title":"Mercedes-Benz E-Klasse E200 Kompressor","year":"2003","km":"157 000 km","price":"76 538,-"},{"finnkode":"84633245","location":"Oslo","title":"Mercedes-Benz C-Klasse 22O CDI EU TIL 11 2017","year":"2003","km":"238 000 km","price":"44 900,-"},{"finnkode":"84594833","location":"Oslo","title":"Rover 75 1,8 T 150hk stv","year":"2003","km":"178 118 km","price":"30 538,-"},{"finnkode":"84584081","location":"Varhaug","title":"Peugeot 307 SW 7 seter 109hk panorama ny service","year":"2003","km":"204 000 km","price":"36 538,-"},{"finnkode":"84165544","location":"Sola","title":"Audi A4 1.8T 163HK,Skinn,Krok,Klima,EU1/18!","year":"2003","km":"297 000 km","price":"24 000,-"},{"finnkode":"84103405","location":"Skien","title":"Mercedes-Benz C-Klasse C200 Cdi Elegance , Luke , H feste","year":"2003","km":"306 000 km","price":"36 000,-"},{"finnkode":"84084920","location":"Trondheim","title":"Volvo V70 D5 aut // Skinn // Ny regreim // PEN //","year":"2003","km":"290 000 km","price":"65 000,-"},{"finnkode":"83812762","location":"Trondheim","title":"Volvo XC 70 2.4D5 AWD Summum","year":"2003","km":"265 000 km","price":"99 000,-"},{"finnkode":"83790821","location":"Moss","title":"Mercedes-Benz E-Klasse E 270, 177 HK, SKINN, XENON, EU OK,","year":"2003","km":"241 500 km","price":"89 900,-"},{"finnkode":"83790179","location":"Kolsås","title":"Volvo XC 90 2.5 T","year":"2003","km":"231 000 km","price":"91 538,-"},{"finnkode":"83731633","location":"Grimstad","title":"Mercedes-Benz E-Klasse E220CDI Avantgarde Aut/Skinn","year":"2003","km":"295 000 km","price":"79 000,-"},{"finnkode":"83622459","location":"Singsås","title":"Toyota Avensis 1,8-stv-Krok-129hk","year":"2003","km":"172 000 km","price":"45 000,-"},{"finnkode":"83449263","location":"Harstad","title":"Mercedes-Benz E-Klasse E220 CDI TDTE+","year":"2003","km":"378 028 km","price":"24 000,-"},{"finnkode":"82819382","location":"Sandnes","title":"Mazda 6 1,8i 120Hk EU ok 11/2017","year":"2003","km":"198 000 km","price":"29 000,-"},{"finnkode":"83345302","location":"Reinsvoll","title":"Volkswagen Passat 1.6 102 hk Stasjonsvogn","year":"2003","km":"196 000 km","price":"8 538,-"},{"finnkode":"80390042","location":"Fjerdingby","title":"Cadillac Escalade ESV","year":"2003","km":"146 500 km","price":"276 538,-"},{"finnkode":"83324405","location":"Porsgrunn","title":"Volvo V40 1.6 Velholdt eu-ok","year":"2003","km":"237 000 km","price":"25 038,-"},{"finnkode":"83033536","location":"Moelv","title":"Mitsubishi Galant 2.0 AUTOMAT, HENGERFESTE, NY REGREIM.AC","year":"2003","km":"189 350 km","price":"49 000,-"},{"finnkode":"82969202","location":"Siljan","title":"BMW 5-serie 520D","year":"2003","km":"242 000 km","price":"39 538,-"},{"finnkode":"82925461","location":"Drammen","title":"Volkswagen Passat Turbo D Sport","year":"2003","km":"250 000 km","price":"39 999,-"},{"finnkode":"82504798","location":"Førde","title":"Mercedes-Benz C-Klasse C200 CDI STV. LAV KM.","year":"2003","km":"157 400 km","price":"89 900,-"},{"finnkode":"82390180","location":"Levanger","title":"Mercedes-Benz E-Klasse Sprek, pen og komfortabel,","year":"2003","km":"257 000 km","price":"89 900,-"},{"finnkode":"82195738","location":"Kleppe","title":"Volvo V70 EU OK TIL 03. 2019 | D5 - 163HK | AUTOMAT | SKINN | LAV GARANTERT KM | GARANTI | TOPPUTGAVE | REGREIM BYTTET 2014 | ALLE SERVICER | DAB+ | HENGERFESTE 1800KG | VI LEVERER GUNSTIG OVER HELE NORGE | 100% FINANS |","year":"2003","km":"191 500 km","price":"74 900,-"},{"finnkode":"82168595","location":"Skien","title":"Renault Laguna 1.8L16V120HK DYNAMIQUE DEL SKINN KL","year":"2003","km":"173 000 km","price":"36 500,-"},{"finnkode":"81887428","location":"Aksdal","title":"Volkswagen Passat 1,8 Highline","year":"2003","km":"232 000 km","price":"30 538,-"},{"finnkode":"81801204","location":"Karmsund","title":"Porsche Cayenne S Velholdt, 21\", Bose, Skinn, Navi, El. svingbart h.feste, Cruisekontroll m.m.","year":"2003","km":"225 000 km","price":"279 000,-"},{"finnkode":"81694485","location":"Hagan","title":"Audi A4 2.0/131hk/manuell/avant/Ny-EU","year":"2003","km":"228 758 km","price":"33 538,-"},{"finnkode":"81674382","location":"Oslo","title":"Skoda Fabia EU TIL 11 2016","year":"2003","km":"125 000 km","price":"29 538,-"},{"finnkode":"81428257","location":"Oslo","title":"Audi A6 1,8 5VT 1,8 TURBO ST.V PEN, NORSK, RECARO SKINN F/B, BOSE, OPPGRADERT PROGRAMVARE+++","year":"2003","km":"191 000 km","price":"69 900,-"},{"finnkode":"80982562","location":"Trondheim","title":"Skoda Fabia 1,4MPi","year":"2003","km":"177 000 km","price":"26 538,-"},{"finnkode":"80949189","location":"Oslo","title":"Subaru Impreza 1.6i TS Stv. 4WD, Eu til Sep 2017","year":"2003","km":"185 000 km","price":"25 000,-"},{"finnkode":"80778298","location":"Sandefjord","title":"Mercedes-Benz C-Klasse 220 CDI Automat Skinn Ny EU & Serv","year":"2003","km":"190 760 km","price":"69 000,-"},{"finnkode":"80327057","location":"Spydeberg","title":"Ford Galaxy 1,9 TDI 7 SETER","year":"2003","km":"268 400 km","price":"11 538,-"},{"finnkode":"80277849","location":"Melhus","title":"Volvo XC 70 2.4D 163HK","year":"2003","km":"281 163 km","price":"89 000,-"},{"finnkode":"79760314","location":"Randaberg","title":"Alfa Romeo 156 140HK JTD16V Skinn Krok Ny EU alu","year":"2003","km":"159 500 km","price":"29 900,-"},{"finnkode":"79292572","location":"Oslo","title":"Audi A4 1.6 Pen bil!","year":"2003","km":"215 000 km","price":"54 538,-"},{"finnkode":"78446753","location":"Grinder","title":"Mercedes-Benz E-Klasse E220","year":"2003","km":"146 188 km","price":"129 000,-"},{"finnkode":"78055008","location":"Sandnes","title":"Mitsubishi Outlander 2,0 Comfort + Meget velholdt bil","year":"2003","km":"180 000 km","price":"64 000,-"},{"finnkode":"77937541","location":"Vestby","title":"Nissan Primera 1.6*109HK*EU OK 30.11.2017*","year":"2003","km":"173 000 km","price":"23 900,-"}]""")
      val numOfCars = jsonCarHdr.as[JsArray].value.size //gets value as [{CAR1},{CAR2}]
      val acqCarHeaderList = Range(0, numOfCars).map { i =>
        val finnkode = jsonCarHdr(i).\("finnkode").asOpt[String].getOrElse(Utility.Constants.EmptyString).toInt
        val location = jsonCarHdr(i).\("location").asOpt[String].getOrElse(Utility.Constants.EmptyString)
        val title = jsonCarHdr(i).\("title").asOpt[String].getOrElse(Utility.Constants.EmptyString)
        val year = jsonCarHdr(i).\("year").asOpt[String].getOrElse(Utility.Constants.EmptyString)
        val km = jsonCarHdr(i).\("km").asOpt[String].getOrElse(Utility.Constants.EmptyString)
        val price = jsonCarHdr(i).\("price").asOpt[String].getOrElse(Utility.Constants.EmptyString)
        val url = generateFinnCarUrl(finnkode)
        val load_time = System.currentTimeMillis()
        val load_date = truncDate(new java.util.Date(load_time)).toInstant().atZone(ZoneId.systemDefault()).toEpochSecond*1000
        //val load_date = new java.sql.Date(load_time)
        AcqCarHeader(finnkode=finnkode, location=location, title = title, year=year, km=km, price=price, load_time=load_time, load_date=load_date, url=url)
      }
      acqCarHeaderList.toList

  }

  def generateFinnCarUrl (finnkode:Int): String = {
    //val finnkode = 88450076
    val baseUrl = "http://m.finn.no/car/used/ad.html?finnkode="
    val finnCarUrl = baseUrl + finnkode.toString
    finnCarUrl
  }



  def getURL(url: String)(retry: Int): Try[Document] = {
    Try(Jsoup.connect(url).get)
      .recoverWith {
        case _ if(retry > 0) => {
          Thread.sleep(3000)
          println("Retry url " + url + " - " + retry + " retries left")
          getURL(url)(retry - 1)
        }
      }
  }
  def scrapeCarDetails(url:String):Map[String, JsValue]= {
    //val url = "http://m.finn.no/car/used/ad.html?finnkode=78647939"
    //val url = "http://m.finn.no/car/used/ad.html?finnkode=77386827" //sold
    //val url = "http://m.finn.no/car/used/ad.html?finnkode=78601940" //deleted page
    val validUrl = url.replace("\"", "")
    val doc: Try[Document] = getURL(validUrl)(10)
    doc match {
      case Success(doc) =>
        val carPropElements: Element = doc.select(".mvn+ .col-count2from990").first()
        var carPropMap = Map[String, String]()

        if (carPropElements != null) {
          var i = 0
          for (elem: Element <- carPropElements.children()) {
            if ((i % 2) == 0) {
              val key = elem.text
              val value = elem.nextElementSibling().text
              carPropMap += (key.asInstanceOf[String] -> value.asInstanceOf[String])
            }
            i = i + 1
          }
        } else carPropMap = Map("MissingKeys" -> "MissingValues")

        var carEquipListBuffer: ListBuffer[String] = ListBuffer()
        val carEquipElements: Element = doc.select(".col-count2upto990").first()
        if (carEquipElements != null) {
          for (elem: Element <- carEquipElements.children()) {
            carEquipListBuffer += elem.text
          }
        } else carEquipListBuffer = ListBuffer("MissingValues")

        val carInfoElements: Element = doc.select(".object-description p[data-automation-id]").first()
        val carInfoElementsText = {
          if (carInfoElements != null) {
            carInfoElements.text
          } else "MissingValues"
        }

        val carPriceElement: Element = doc.select(".mtn.r-margin").first()
        val carPriceText = {
          if (carPriceElement != null) carPriceElement.text else "MissingValue"
        }

        val carTitleElement: Element = doc.select(".tcon").first()
        val carTitleText = {
          if (carTitleElement != null) carTitleElement.text else "MissingValue"
        }

        val carLocationElement: Element = doc.select(".hide-lt768 h2").first()
        val carLocationText= {
          if (carLocationElement != null) carLocationElement.text else "MissingValue"
        }

        val carYearElement: Element = doc.select("hr+ .col-count2from990 dd:nth-child(2) , .mvn+ .col-count2from990 dt:nth-child(2)").first()
        val carYearText= {
          if (carYearElement != null) carYearElement.text else "MissingValue"
        }

        val carKMElement: Element = doc.select(".mvn+ .col-count2from990 dd:nth-child(6)").first()
        val carKMText = {
          if (carKMElement != null) carKMElement.text else "MissingValue"
        }

        val jsObj = Json.obj("url" -> url, "properties" -> carPropMap, "information" -> carInfoElementsText, "equipment" -> carEquipListBuffer.toList, "deleted" -> false, "title" -> carTitleText, "location" -> carLocationText, "price" -> carPriceText, "year" -> carYearText, "km" -> carKMText)
        jsObj.value.toMap
      case Failure(e) => {
        println("URL " + url + " has been deleted.")
        val jsObj = Json.obj("url" -> url, "properties" -> Map("NULL" -> "NULL"), "information" -> "NULL", "equipment" -> ListBuffer("NULL").toList, "deleted" -> true, "title" -> "NULL", "location" -> "NULL", "price" -> "NULL", "year" -> "NULL", "km" -> "NULL")
        jsObj.value.toMap
      }
    }
  }


  def getMapFromJsonMap(jsonString:String, excludedKeys:Seq[String]=Seq("None")):HashMap[String,String] = {
    //    val keys = Seq("Salgsform", "Girkasse")
    //    val jsonString = "{\"Salgsform\":\"Bruktbil til salgs\",\"Girkasse\":\"Automat\",\"Antall seter\":\"5\"}"
    //val jsonString = """{"Salgsform":"Bruktbil til salgs"}"""
    //val excludedKeys=Seq("")
    val jsValueMap: JsValue = Json.parse(jsonString)
    val propertiesMap = jsValueMap.as[Map[String,String]]
    val hashMap = new HashMap[String, String]
    propertiesMap.map{case(k,v) => if (!excludedKeys.contains(k)) hashMap.put(k,v) }
    hashMap
  }

  def getSetFromJsonArray(jsonString:String, excludedElements:Seq[String]=Seq("None")):Set[String] = {
    //    val elements = Seq("Automatisk klimaanlegg", "Skinnseter")
    val jsValueArray:JsValue = Json.parse(jsonString)
    val set = jsValueArray.as[Set[String]]
    set.filter(x => !excludedElements.contains(x))
  }

  def getStringFromJsonString(jsonString:String):String = {
    //    val jsonString = "\"Fin bil\""
    Json.parse(jsonString).as[String]
  }

  def setupCassandraTestKeyspace() = {
    val conf = new SparkConf().setAppName("Testing").setMaster("local[*]").set("spark.cassandra.connection.host", "192.168.56.56")
    val ddl_prod = Source.fromFile("C:\\Users\\torbjorn.torbjornsen\\IdeaProjects\\finnCarsSpark\\c.ddl").getLines.mkString
    val ddl_test = ddl_prod.replace("finncars", "test_finncars")
    val ddl_test_split = ddl_test.split(";")
    val ddl_test_cmds = ddl_test_split.map(elem => elem + ";")

    CassandraConnector(conf).withSessionDo { session =>
      ddl_test_cmds.map { cmd =>
        println(cmd)
        session.execute(cmd)
      }
    }
  }

  def parseKM(km:String):Int = {
    //val km = "99 000 km"
    val parsedKM = km.replaceAll("[^\\x00-\\x7F]", "").replace(" ", "").replace("km", "").replace("\"", "")
    if (parsedKM.forall(_.isDigit)) parsedKM.toInt else -1
  }

  def parseFinnkode(url:String):Int = {
    //val url = "http://m.finn.no/car/used/ad.html?finnkode=72921101"
    val parsedFinnkode = url.substring(url.length-8,url.length).replace("\"", "")
    if (parsedFinnkode.forall(_.isDigit)) parsedFinnkode.toInt else -1
  }


  def parseYear(year:String):Int = {
    //val year = "2007"
    val parsedYear = year.replace("\"", "").replace("\"", "")
    if (parsedYear.forall(_.isDigit)) parsedYear.toInt else -1
  }

  def carMarkedAsSold(price:String):Boolean = {
    if (price == "Solgt") true else false
  }

  def getDaysBetweenStringDates(dateStart:String, dateFinish:String):Int = {
    if (dateFinish == Utility.Constants.EmptyDate) {
      Utility.Constants.EmptyInt
    } else LocalDate.parse(dateStart).until(LocalDate.parse(dateFinish), ChronoUnit.DAYS).toInt
  }
  def getDatesBetween(dateStart:LocalDate, dateEnd:LocalDate):Seq[String] = {
    //val dateStart = LocalDate.now
    //val dateEnd = LocalDate.now.plusDays(-365)
    val daysBetween = dateStart.until(dateEnd, ChronoUnit.DAYS).toInt
    val listOfDays = ListBuffer[String]()

    for (i <- 0 to daysBetween by 1) {
      listOfDays += (dateStart.plusDays(i)).toString
    }

    listOfDays.toList
  }

  def hasAutomatgir(properties:HashMap[String, String]):Boolean = {
    properties.get("Girkasse") == "Automat"
  }

  def hasHengerfeste(equipment:Set[String]):Boolean = {
    equipment.contains("Hengerfeste") || equipment.contains("Tilhengarfeste") || equipment.contains("Tilhengerfeste")
  }

  def getSkinninterior(equipment:Set[String]):String = {
    if (equipment.contains("Skinninteriør") || equipment.contains("Skinnseter")) {
      "Skinnseter"
    } else if (equipment.contains("Delskinn")) {
      "Delskinn"
    } else "Tøyseter"
  }

  def getDrivstoff(properties:HashMap[String, String]):String= {
    properties.get("Drivstoff")
  }

  def getSylindervolum(properties:HashMap[String, String]):Double= {
    val text = properties.getOrElse("Sylindervolum", Utility.Constants.EmptyInt.toString)
    val parsedText = text.replaceAll("[A-Za-z\\s ]", "").replace(",",".")
    parseDouble(parsedText) match {
      case Some(d) => d
      case None => Utility.Constants.EmptyInt
    }
  }

  def getEffekt(properties:HashMap[String, String]):Int= {
    val text = properties.getOrElse("Effekt", Utility.Constants.EmptyInt.toString)
    text.replaceAll("[A-Za-z\\s]", "").toInt
  }

  def hasRegnsensor(equipment:Set[String]):Boolean = {
    equipment.contains("Regnsensor")
  }

  def getFarge(properties:HashMap[String, String]):String= {
    properties.get("Farge")
  }

  def hasCruisekontroll(equipment:Set[String]):Boolean = {
    equipment.contains("Cruisekontroll")
  }

  def hasParkeringsensor(equipment:Set[String]):Boolean = {
    equipment.contains("Parkeringsensor") || equipment.contains("Parkeringsensor bak") || equipment.contains("Parkeringsensor foran")
  }

  def getAntallEiere(properties:HashMap[String, String]):Int= {
    properties.getOrElse("Antall eiere", Utility.Constants.EmptyInt.toString).toInt
  }

  def getKommune(location:String):String = {
    Utility.Constants.EmptyString
  }


  def getFylke(location:String):String = {
    Utility.Constants.EmptyString
  }

  def hasXenon(equipment:Set[String]):Boolean = {
    equipment.contains("Xenon")
  }

  def hasNavigasjon(equipment:Set[String]):Boolean = {
    equipment.contains("Navigasjonssystem")
  }

  def hasServicehefte(description:String):Boolean = {
    description.contains("servicehefte") || description.contains("Servicehefte") || description.contains("service hefte")
  }

  def hasSportsseter(equipment:Set[String]):Boolean = {
    equipment.contains("Sportsseter")
  }

  def hasTilstandsrapport(properties:HashMap[String, String]):Boolean= {
    properties.containsKey("Tilstandsrapport")
  }

  def getVekt(properties:HashMap[String, String]):Int= {
    val text = properties.getOrElse("Vekt", Utility.Constants.EmptyInt.toString)
    text.replaceAll("[\\D]", "").toInt
  }

  def removeSpecialCharacters(text:String):String = {

    "test"
  }


  def propCarToString(p:Product):String = {
    p.productIterator.map {
      case s: String => "\"" + s + "\""
      case hm: HashMap[_, _] => "new HashMap[String,String](Map" + hm.map(t => "\"" + t._1 + "\"" + "->" + "\"" + t._2 + "\"") + ")" //cannot convert HashMap without first specifying a Scala map
      case set: Set[_] => set.map(v => "\"" + v + "\"")
      case l:Long => l.toString + "L"
      case other => other.toString
    }.mkString (p.productPrefix + "(", ", ", ")").replace("ArrayBuffer", "")
  }

  def propCarToStringAndKey(p:Product, url:String):(String,String) = {
    (url, p.productIterator.map {
      case s: String => "\"" + s + "\""
      case hm: HashMap[_, _] => "new HashMap[String,String](Map" + hm.map(t => "\"" + t._1 + "\"" + "->" + "\"" + t._2 + "\"") + ")" //cannot convert HashMap without first specifying a Scala map
      case set: Set[_] => set.map(v => "\"" + v + "\"")
      case l:Long => l.toString + "L"
      case other => other.toString
    }.mkString (p.productPrefix + "(", ", ", ")").replace("ArrayBuffer", ""))
  }


  def createPropCar(acqCarH:Dataset[AcqCarHeader], acqCarD:Dataset[AcqCarDetails]):PropCar = {
    val firstAcqCarH = acqCarH.orderBy(desc("load_date")).first
    val firstAcqCarD = acqCarD.orderBy(desc("load_date")).first
    val finnkode = firstAcqCarH.finnkode
    val load_date = firstAcqCarH.load_date*1000
    val title = firstAcqCarH.title.replace("|","")
    val location = firstAcqCarH.location
    val year = parseYear(firstAcqCarH.year)
    val km = parseKM(firstAcqCarH.km)
    val price = parsePrice(firstAcqCarH.price)
    val properties = getMapFromJsonMap(firstAcqCarD.properties.replace("\"{", "{").replace("}\"", "}").replace("|",""))
    val equipment = getSetFromJsonArray(firstAcqCarD.equipment.replace("\"[", "[").replace("]\"", "]").replace("|",""))
    val information = firstAcqCarD.information.replace("|","")
    val sold = carMarkedAsSold(price)
    val deleted = false //TODO:How to identify?
    val load_time = System.currentTimeMillis()
    val url = firstAcqCarH.url

    PropCar(finnkode=finnkode,load_date=load_date,title=title,location=location,year=year,km=km,price=price,properties=properties,equipment=equipment,information=information,sold=sold,deleted=deleted,load_time=load_time,url=url)


  }

  def parsePrice(price:String):String = {

    val parsedPrice = if (price == "Solgt") "Solgt" else {
      val tempPrice = price.replace(",-","").replaceAll("[^\\x00-\\x7F]", "").replace(" ","").replace("\"", "")
      if (tempPrice.forall(_.isDigit)) tempPrice.toString else "-1" //price invalid
    }
    parsedPrice
  }

  def truncDate(date:java.util.Date):java.util.Date = {
    val cal = Calendar.getInstance()
    cal.setTime(date)
    cal.set(Calendar.HOUR_OF_DAY,0)
    cal.set(Calendar.MINUTE,0)
    cal.set(Calendar.SECOND,0)
    cal.set(Calendar.MILLISECOND,0)
    cal.getTime()
  }

  def saveToCSV(rdd:RDD[org.apache.spark.sql.Row]) = {
    val temp = rdd.map(row => row.mkString("|"))
    temp.coalesce(1).saveAsTextFile("/usr/temp/")
  }

  def parseDouble(s:String): Option[Double] = {
    Try {s.toDouble}.toOption
  }


  def printCurrentMethodName() : Unit = println(Thread.currentThread.getStackTrace()(2).getMethodName)

  object Constants {
    val EmptyMap = new java.util.HashMap[String,String](Map("NULL" -> "NULL"))
    val EmptyList = Set("NULL")
    val EmptyString = "NULL"
    val EmptyInt = -1
    val EmptyDate = "1900-01-01"
    val EmptyUtilDate = new java.sql.Date(1900,1,1)
    val ETLSafetyMargin = 7 //days
    val ETLFirstLoadDate = LocalDate.of(2016,7,1)
  }
}
