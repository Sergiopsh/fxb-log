package main

import (
    "fmt"
    "github.com/hpcloud/tail"
    regex "regexp"
    elasticapi "gopkg.in/olivere/elastic.v3"
    "time"
    "strings"
    "os"
)

var logNAME             string = "/var/log/fxbank/cluster-logs/fxbank-ws.log"
var newStringPtrn       string = "^\\(serverId=([0-9]+)\\) ([0-9]{4}-[0-9]{2}-[0-9]{2}) ([0-9]{2}:[0-9]{2}:[0-9]{2}[:0-9]*) "
var elasticUrl          string = "http://185.19.218.2:9289"

var elasticClient       *elasticapi.Client = nil;

func elasticInit(){
    var err error
    elasticClient = nil
    for elasticClient == nil {
        elasticClient, err = elasticapi.NewClient(
            elasticapi.SetURL(elasticUrl),
            elasticapi.SetSniff(false))
        if err != nil {
            fmt.Println(err)
            elasticClient = nil
            time.Sleep(5 * time.Second)
        }
    }

}
func elasticPing(){
    var status bool = false;
    for status == false{
        status = elasticClient.IsRunning()
        if !status {
            elasticInit()
        }
    }

}

var ArrMap map[string]map[string]string

func main() {

    //get first arg as file_name

    if len(os.Args)==2 {
        logNAME = os.Args[1]
    }
    fmt.Print(logNAME)
    ArrMap = make (map[string]map[string]string)

    elasticInit()
    var c chan []string = make(chan []string)
    go routine(c);
    go routine(c);
    go routine(c);
    go routine(c);
    go routine(c);
    go routine(c);
    go routine(c);
    go routine(c);


    var messMap []string
    tc := tail.Config{Follow: true, ReOpen: true, MustExist: true, Poll: true}
    t, _ := tail.TailFile(logNAME, tc  )
    for line := range t.Lines {
        //try to find start INBOUND or OUBOUND message
        if res,_ := regex.MatchString(newStringPtrn + ".+ Message$",line.Text); res == true{
            if messMap != nil{
                //sendES(messMap)
                c <- messMap

            }
            //Drop okd message from map
            messMap = nil
            messMap = append(messMap,line.Text)
            continue
        }else if res,_ := regex.MatchString(newStringPtrn + ".+$",line.Text); res == true && messMap != nil {
            //if new string have something like this (serverId=11) 2015-12-29 00:00:49 THEN this NEW message from APP
            //sendES(messMap)
            c <- messMap
            messMap = nil
            continue
        }
        if res,_ := regex.MatchString(newStringPtrn,line.Text); res == false && messMap != nil {//MESSAGE BODY
            if res,_ := regex.MatchString("^[A-Za-z-]+: .+",line.Text); res == true {
                messMap = append(messMap, line.Text)
            }else {
                messMap[len(messMap)-1] = messMap[len(messMap)-1] + line.Text
            }
            continue
        }
    }
}

func routine(c chan []string){
    for {
        sendES(<- c)
    }

}



func sendES(messMap []string){
    sendArr := make(map[string]string)

    isPrevExist := false

    refirst := regex.MustCompile(newStringPtrn + ".+ (.+?) Message[-]*$")
    strfirst := refirst.FindAllStringSubmatch(messMap[0], -1)
    if len(strfirst[0])< 5 {
        return
    }
    sendArr[strfirst[0][4]+"_"+"ServerID"] = strfirst[0][1]
    //sendArr[strfirst[0][4]+"_"+"timestamp"] = strfirst[0][2] + " " + strfirst[0][3]
    tt, _ := time.Parse("2006-01-02 15:04:05", strfirst[0][2] + " " + strfirst[0][3])
    sendArr["timestamp"] = tt.Format("2006-01-02T15:04:05.000Z07:00")



    re := regex.MustCompile("^(.+?): (.+)$")

    for _,mess := range messMap {
        if res,_ := regex.MatchString(newStringPtrn + ".+$",mess); res == true{
            continue
        }
        arr := re.FindAllStringSubmatch(mess, -1)
        if len(arr) < 1 {
            continue
        }
        if len(arr[0]) < 2 {
            continue
        }
        if arr[0][1] == "ID" {
            sendArr["fxb_ID"] = strings.Replace(arr[0][2],"-","",-1);
        }else {
            sendArr[strfirst[0][4] + "_" + arr[0][1]] = arr[0][2];
        }

    }

    var ok bool = false

    //if Outbound Message and NOT Http-Method
    if strfirst[0][4] == "Outbound"{
        isPrevExist = true
        for key,_ := range sendArr{
            if key == "Outbound_Http-Method"{
                isPrevExist = false
            }
        }
    }
    if strfirst[0][4] == "Inbound"{
        isPrevExist = true
        for key,_ := range sendArr{
            if key == "Inbound_Http-Method"{
                isPrevExist = false
            }
        }
    }
    //рассматривать стоит только сообщения с длиной id > 15
    if len(sendArr["fxb_ID"])>15 {
        //отправлять необходимо только сообщения, у которых удалось найти и InBound + OutBound
        //try to find parent element
        if isPrevExist == true {
            //У элемента есть родительский элемент в ArrMap.
            // Необходимо найти этот элемент в ArrMap
            // к нему приклеить текущий
            // и отправить в ES
            // и удалить родительский в ArrMap

            // Необходимо найти этот элемент в sendArr

            foundArr := make(map[string]string)

            if len(ArrMap[sendArr["fxb_ID"]])<1{
                //родительский элемент не найден
                return
            }
            foundArr = ArrMap[sendArr["fxb_ID"]]

            // к нему приклеить текущий SendArr + FoundArr


            for key, val := range foundArr {
                sendArr[key] = val
            }

            //Не нужно посылать в Elastic

            if sendArr["Inbound_Address"] == "http://secure-local.fxclub.org:8099/public_api/refreshSession" {
                delete(ArrMap,sendArr["fxb_ID"])
                return
            }

            //send to Elastic
            ok = false
            for ok == false {
                elasticPing()
                _, err := elasticClient.Index().
                Index(strfirst[0][2]).
                Type("ws-log").
                BodyJson(sendArr).
                Do()
                if err == nil {
                    ok = true
                }else {
                    fmt.Println(err)
                    time.Sleep(1 * time.Second)
                }
            }
            // и удалить родительский в ArrMap
            delete(ArrMap,sendArr["fxb_ID"])
        }else{
            //У элемента нет родительского элемента. Ложим его в ArrMap
            ArrMap[sendArr["fxb_ID"]] = sendArr
        }



    }

}
