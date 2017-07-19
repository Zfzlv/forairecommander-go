package main

import (
    _ "github.com/go-sql-driver/mysql"
    "github.com/go-xorm/xorm"
    "fmt"
    "log"
    "bytes"
    "time"
    "sync"
    "strconv"
    "github.com/streadway/amqp"
    "encoding/json"
    "github.com/go-redis/redis"
)

var conn *amqp.Connection
var channel *amqp.Channel
var count = 0
var en_local *xorm.Engine
var redisync SafeRedis
var t1 time.Time
const Trycount = 5
const Dayscount = 5
func init() {  
    var err error  
    t1 = time.Now()
    fmt.Println("-开始运行-",time.Now().String())
    en_local, err = xorm.NewEngine("mysql", "wenba:KDAN82aw5g2XDNK4SQ6RsuEc4pl9DtBH@tcp(rm-bp1e92v83gxr464y6o.mysql.rds.aliyuncs.com:3306)/zujuan?charset=utf8")
    en_local.SetMaxIdleConns(5)
    if err != nil {
        panic(err.Error())
    }
    //en_local.ShowSQL(true) 
    fmt.Println("--已连上本地库---") 
    redisync.redis_pool = redis.NewClient(&redis.Options{
        Addr:     "10.2.1.160:6379",
        Password: "",
        DB:       0,
        PoolSize: 5,
    })
    pong, err := redisync.redis_pool.Ping().Result()
    if err != nil && pong=="Pong" {
        panic(err.Error())
    }
    fmt.Println("--已连上Redis---")
}  

type SafeRedis struct {
    redis_pool   *redis.Client
    mux sync.Mutex
}

type Tangseng struct {  
    Id    string  `xorm:"varchar(255) pk notnull unique 'id'"`
    Types    int  `xorm:"tinyint(2) 'types'"`
    Stu_id    int  `xorm:"int(11) 'stu_id'"`
    Source_type    int  `xorm:"tinyint(3) 'source_type'"`
    Source_id    string  `xorm:"varchar(50) 'source_id'"`
    Subject    int  `xorm:"tinyint(4) 'subject'"`
    Stu_score_percent    int  `xorm:"tinyint(4) 'stu_score_percent'"`
    Stu_score    string   `xorm:"varchar(10) 'stu_score'"`
    Used_time    int  `xorm:"int(11)  'used_time'"`
    Submit_time    int  `xorm:"int(11) 'submit_time'"`
    Status    int  `xorm:"tinyint(4) 'status'"`
    Original_id  string   `xorm:"varchar(50) index unique 'original_id'"`
}  
type TangsengQuestions struct {  
    Id    string   `xorm:"varchar(100) pk notnull unique 'id'"`
    Number    int  `xorm:"int(11) 'number'"`
    Question_id    int  `xorm:"int(11) 'question_id'"`
    Question_type    int  `xorm:"int(11) 'question_type'"`
    Stu_answer    string  `xorm:"text 'stu_answer'"`
    Score    int  `xorm:"tinyint(3) 'score'"`
    Stu_score    string  `xorm:"varchar(10) 'stu_score'"`
    Question_score    int  `xorm:"int(11) 'question_score'"`
    Used_time    int  `xorm:"int(11)  'used_time'"`
    Submit_time    int  `xorm:"int(11) 'submit_time'"`
    Created_at time.Time `xorm:"timestamp 'created_at'"`
    Status    int  `xorm:"tinyint(4) 'status'"`
    Is_submit int  `xorm:"tinyint(4) 'is_submit'"`
}
type TangsengMiddle struct {  
    Tangseng_id    string `xorm:"varchar(255) notnull index 'tangseng_id'"`
    Global_id    string `xorm:"varchar(100) index 'global_id'"`
}

type Datas struct{
    Id              string   `json:"id"`
    Timestamp              int      `json:"timestamp"`
    Type     int   `json:"type"`
    Stu_id int      `json:"stu_id"`
    Source_type         int   `json:"source_type"`
    Source_id      int   `json:"source_id"`
    Subject  int   `json:"subject"`
    Stu_score_percent  int      `json:"stu_score_percent"`
    Stu_score  int      `json:"stu_score"`
    Used_time  int      `json:"used_time"`
    Submit_time  int      `json:"submit_time"`
    Questions            []Datas_Questions `json:"questions"`
}

type Datas_Questions struct{
    Number              int   `json:"number"`
    Question_id              int      `json:"question_id"`
    Question_type     int   `json:"question_type"`
    Stu_answer string      `json:"stu_answer"`
    Score         int   `json:"score"`
    Stu_score      int   `json:"stu_score"`
    Question_score  int   `json:"question_score"`
    Used_time  int      `json:"used_time"`
    Submit_time  int      `json:"submit_time"`
}

type DataSingle struct{
    Global_id string
    Stu_id int
    Score int
    Stu_score string
    Question_score int
    Used_time int
    Submit_time int
    Source_type int
    Is_submit int
}

type DataS struct{
    Score int
    Stu_score string
    Question_score int
    Used_time int
    Submit_time int
    Is_submit int
}

type Rabbitmqs struct {  
    Id    string   `xorm:"varchar(255) pk notnull unique 'id'"`
    Timestamps    int  `xorm:"int(11) 'timestamps'"`
    Types    int  `xorm:"tinyint(4) 'types'"`
    Stu_id    int  `xorm:"int(11) 'stu_id'"`
    Source_type    int  `xorm:"tinyint(4) 'source_type'"`
    Source_id    int  `xorm:"int(11) 'source_id'"`
    Subject    int  `xorm:"tinyint(4) 'subject'"`
    Stu_score_percent    int  `xorm:"tinyint(4) 'stu_score_percent'"`
    Stu_score    int  `xorm:"tinyint(4)  'stu_score'"`
    Used_time    int  `xorm:"int(11) 'used_time'"`
    Submit_time    int  `xorm:"int(11) 'submit_time'"`
    Questions string  `xorm:"text 'questions'"`
}
const (
    queueName = "from_ai_to_recommend"
    exchange  = "aixue_recommend"
    mqurl ="amqp://wenba:mqwenba@121.43.166.128:5672"
)

func main() {
    /*go func() { 
        for {
            push()
            time.Sleep(1 * time.Second)
        }
    }()*/
    receive()
    fmt.Println("end")
    close()
}

func failOnErr(err error, msg string) {
    if err != nil {
        log.Fatalf("%s:%s", msg, err)
        panic(fmt.Sprintf("%s:%s", msg, err))
    }
}

func mqConnect() {
    var err error
    conn, err = amqp.Dial(mqurl)
    failOnErr(err, "failed to connect tp rabbitmq")

    channel, err = conn.Channel()
    failOnErr(err, "failed to open a channel")
}

func close() {
    channel.Close()
    conn.Close()
}

//连接rabbitmq server
func push() {

    if channel == nil {
        mqConnect()
    }
    msgContent := "hello aixue_recommend!"

    channel.Publish(exchange, queueName, false, false, amqp.Publishing{
        ContentType: "text/plain",
        Body:        []byte(msgContent),
    })
}

func createTangsengQuestion(questions []TangsengQuestions ) {  
    fmt.Println("++++++++++++++++++++ Insert into TangsengQuestions ++++++++++++++++++++")   
    var (  
        num int64  
        err error  
    )  
    if num, err = en_local.Insert(questions); err != nil {  
        fmt.Println("Fail to Insert TangsengQuestion : %v", err)  
    }  
    fmt.Println("Succ to insert TangsengQuestion number : %d\n", num)  
}  
func createTangsengMiddle(middles []TangsengMiddle) {  
    fmt.Println("++++++++++++++++++++ Insert into TangsengMiddle ++++++++++++++++++++")  
    var (  
        num int64  
        err error  
    )  
    if num, err = en_local.Insert(middles); err != nil {  
        fmt.Println("Fail to Insert TangsengMiddle : %v", err)  
    }  
    fmt.Println("Succ to insert TangsengMiddle number : %d\n", num)  
} 
func createTangseng(tangs []Tangseng) int{  
    fmt.Println("++++++++++++++++++++ Insert into Tangseng ++++++++++++++++++++")  
    var (  
        num int64  
        err error  
    )  
    if num, err = en_local.Insert(tangs); err != nil {  
        fmt.Println("Fail to Insert Tangseng : %v", err)  
        return 0
    }  
    fmt.Println("Succ to insert Tangseng number : %d\n", num)
    return 1  
} 

func receive() {
    if channel == nil {
        mqConnect()
    }

    msgs, err := channel.Consume(queueName, "", true, false, false, false, nil)
    failOnErr(err, "")

    forever := make(chan bool)

    go func() {
        //fmt.Println(*msgs)
        for d := range msgs {
            s := BytesToString(&(d.Body))
            count++
            //fmt.Printf("receve msg is :%s -- %d\n", *s, count)
            fmt.Printf("receve msg: %d\n",count)
            var datas Datas
            if err := json.Unmarshal([]byte(*s), &datas); err == nil {
                if len(datas.Questions)>0{
                    var original_id = strconv.Itoa(datas.Source_id)
                        switch datas.Source_type {
                            case 1: original_id = "bigexam_"+original_id
                            case 2: original_id = "homework_"+original_id
                            case 3,4 : original_id = "exercise_"+original_id
                            case 5 : original_id = "before_"+original_id
                            case 6 : original_id = "byclass_"+original_id
                            case 7 : original_id = "recycle_"+original_id
                            case 8 : original_id = "exam_"+original_id
                            default: return  
                        }
                        tmp_t := Tangseng{Original_id:original_id}
                        has_1,err_1 := en_local.Get(&tmp_t);
                        var tangs []Tangseng
                        var questions []TangsengQuestions
                        var middles []TangsengMiddle
                        var dataforredis []DataSingle
                        var dataforupdateredis []DataSingle
                        if err_1 != nil || has_1 == false{
                            tmp_t = Tangseng{
                                Id:datas.Id,
                                Types:datas.Type,
                                Stu_id:datas.Stu_id,
                                Source_type:datas.Source_type,
                                Source_id:strconv.Itoa(datas.Source_id),
                                Subject:datas.Subject,
                                Stu_score_percent:datas.Stu_score_percent,
                                Stu_score:strconv.Itoa(datas.Stu_score),
                                Used_time:datas.Used_time,
                                Submit_time:datas.Submit_time,
                                Status:0,
                                Original_id:original_id}
                            tangs = append(tangs,tmp_t)
                            back := createTangseng(tangs)
                            for back ==0{
                               tmp_t = Tangseng{Original_id:original_id}
                               has_1,err_1 = en_local.Get(&tmp_t); 
                               if err_1 != nil || has_1 == false{
                                 back = createTangseng(tangs)
                               }else{
                                 back=1
                               }
                            } 
                        }
                        var fla = 0
                        if datas.Type==2 {
                            if tmp_t.Types != datas.Type{
                                fla = 1
                            }
                            tmp_t.Types = datas.Type
                        }
                        if tmp_t.Stu_score != strconv.Itoa(datas.Stu_score) && datas.Stu_score>0{
                            fla = 1
                            tmp_t.Stu_score = strconv.Itoa(datas.Stu_score)
                        }
                        if tmp_t.Used_time != datas.Used_time && datas.Used_time>0{
                            fla = 1
                            tmp_t.Used_time = datas.Used_time
                        }
                        if  datas.Submit_time > tmp_t.Submit_time {
                            fla = 1
                            tmp_t.Submit_time = datas.Submit_time
                        }
                        if tmp_t.Stu_score_percent != datas.Stu_score_percent && datas.Stu_score_percent>0{
                            fla = 1
                            tmp_t.Stu_score_percent = datas.Stu_score_percent
                        }
                        if fla == 1{
                            tmp_t.Status = 0
                            en_local.Id(tmp_t.Id).Cols("Types","Stu_score","Used_time","Submit_time","Status","Stu_score_percent").Update(&tmp_t)
                        }
                    for _,question := range datas.Questions{
                        var global_id = strconv.Itoa(datas.Stu_id)+"_"+strconv.Itoa(datas.Source_id)+"_"+strconv.Itoa(datas.Source_type)+"_"+strconv.Itoa(question.Question_id)+"_"+strconv.Itoa(question.Question_type)+"_"+strconv.Itoa(datas.Subject)                       
                        tmp_q := TangsengQuestions{Id:global_id}
                        has,err := en_local.Get(&tmp_q);
                        var fla_q = 0
                        var fla_x = 0
                        if err == nil && has == true{
                            if tmp_q.Question_type != question.Question_type{
                                fla_q=1
                               tmp_q.Question_type = question.Question_type
                            }
                            if tmp_q.Score != question.Score && question.Score > 0{
                                fla_q=1
                                fla_x=1
                                tmp_q.Score = question.Score
                            }
                            if datas.Source_type==8 && tmp_q.Stu_score != strconv.Itoa(question.Stu_score){
                                fla_q=1
                                fla_x=1
                                tmp_q.Stu_score = strconv.Itoa(question.Stu_score)
                            }
                            if datas.Source_type==8 && tmp_q.Question_score != question.Question_score{
                                fla_q=1
                                fla_x=1
                                tmp_q.Question_score = question.Question_score
                            }
                            if tmp_q.Used_time != question.Used_time && question.Used_time>0{
                                fla_q=1
                                fla_x=1
                                tmp_q.Used_time = question.Used_time
                            } 
                            if tmp_q.Stu_answer != question.Stu_answer && question.Stu_answer!=""{
                                fla_q=1
                                tmp_q.Stu_answer = question.Stu_answer
                            } 
                            if tmp_q.Submit_time > question.Submit_time{
                                fla_q=1
                                fla_x=1
                                tmp_q.Submit_time = question.Submit_time
                            }
                            if tmp_q.Is_submit != tmp_t.Types{
                                fla_q=1
                                fla_x=1
                                tmp_q.Is_submit = tmp_t.Types
                            }
                            if fla_q==1{
                                tmp_q.Status = 3
                                if datas.Source_type==8{
                                    en_local.Id(global_id).Cols("Question_type","Stu_answer","Score","Stu_score","Question_score","Used_time","Submit_time","Status","Is_submit").Update(&tmp_q)
                                }else{
                                    en_local.Id(global_id).Cols("Question_type","Stu_answer","Score","Used_time","Submit_time","Status","Is_submit").Update(&tmp_q)
                                }
                            }
                            if fla_x==1{
                                tmp_u := DataSingle{ 
                                    Global_id:tmp_q.Id,
                                    Stu_id:tmp_t.Stu_id,
                                    Score:tmp_q.Score,
                                    Stu_score:tmp_q.Stu_score,
                                    Question_score:tmp_q.Question_score,
                                    Source_type:tmp_t.Source_type,
                                    Used_time:tmp_q.Used_time,
                                    Submit_time:tmp_q.Submit_time,
                                    Is_submit:tmp_q.Is_submit}  
                                dataforupdateredis = append(dataforupdateredis,tmp_u)
                            }
                        }else{
                            tmp_q = TangsengQuestions{
                                Id: global_id,
                                Number:question.Number,
                                Question_id:question.Question_id,
                                Question_type:question.Question_type,
                                Stu_answer:question.Stu_answer,
                                Score:question.Score,
                                Stu_score:strconv.Itoa(question.Stu_score),
                                Question_score:question.Question_score,
                                Used_time:question.Used_time,
                                Submit_time:question.Submit_time,
                                Status:0,
                                Is_submit:tmp_t.Types}
                            tmp_r := DataSingle{ 
                                Global_id:tmp_q.Id,
                                Stu_id:tmp_t.Stu_id,
                                Score:tmp_q.Score,
                                Stu_score:tmp_q.Stu_score,
                                Question_score:tmp_q.Question_score,
                                Used_time:tmp_q.Used_time,
                                Submit_time:tmp_q.Submit_time,
                                Source_type:tmp_t.Source_type,
                                Is_submit:tmp_q.Is_submit}
                            questions = append(questions,tmp_q)
                            tmp_m := TangsengMiddle{
                                Tangseng_id:tmp_t.Id,
                                Global_id:global_id}
                            middles = append(middles,tmp_m)
                            dataforredis = append(dataforredis,tmp_r)
                        }
                    }
                    if len(questions) > 0 {
                         createTangsengQuestion(questions) 
                         SetToRedis(dataforredis)
                     } 
                    if len(middles) > 0 {
                         createTangsengMiddle(middles)
                    }
                    if  len(dataforupdateredis) > 0 {
                         UpdateToRedis(dataforupdateredis)
                    }
                }
                bx, _ := json.Marshal(datas.Questions)
                mqs := Rabbitmqs{
                    Id:datas.Id,
                    Timestamps:datas.Timestamp,
                    Types:datas.Type,
                    Stu_id:datas.Stu_id,
                    Source_type:datas.Source_type,
                    Source_id:datas.Source_id,
                    Subject:datas.Subject,
                    Stu_score_percent:datas.Stu_score_percent,
                    Stu_score:datas.Stu_score,
                    Used_time:datas.Used_time,
                    Submit_time:datas.Submit_time,
                    Questions:string(bx)}
                if numx, errx := en_local.Insert(mqs); errx == nil {  
                    fmt.Println("Succ to insert Rabbitmqs number : %d\n", numx)
                }
            }
        }
    }()

    fmt.Printf(" [*] Waiting for messages. To exit press CTRL+C\n")
    <-forever
}

func BytesToString(b *[]byte) *string {
    s := bytes.NewBuffer(*b)
    r := s.String()
    return &r
}

func SetToRedis(das []DataSingle){
    redisync.mux.Lock()
    defer redisync.mux.Unlock()   
    //pl := redisync.redis_pool.Pipeline()
    for i:=0;i<len(das);i++{
        ssx := DataS{
            Score:das[i].Score,
            Stu_score:das[i].Stu_score,
            Question_score:das[i].Question_score,
            Used_time:das[i].Used_time,
            Submit_time:das[i].Submit_time,
            Is_submit:das[i].Is_submit}  
        b, err := json.Marshal(ssx)
        if err != nil {
           fmt.Println("encoding faild---",das[i].Global_id)
           continue
        } else {
            keyone := "all_"+strconv.Itoa(das[i].Stu_id)
            keyonekeys := "all_"+strconv.Itoa(das[i].Stu_id)+"_keys"
            historys := time.Unix(int64(das[i].Submit_time),0).Format("20060102")
            keytwo := strconv.Itoa(das[i].Stu_id) + "_" + historys
            trys := 1
            //pl.LPush(keyone,das[i].Global_id)
            _,errs := redisync.redis_pool.HSet(keyone,das[i].Global_id,string(b)).Result()
            redisync.redis_pool.RPush(keyonekeys,das[i].Global_id)
            for errs!=nil && trys<=Trycount {
                _,errs = redisync.redis_pool.HSet(keyone,das[i].Global_id,string(b)).Result()
                trys++
            }
            if errs!=nil{
                fmt.Println("新增：Redis--all_"+strconv.Itoa(das[i].Stu_id)+"--失败---",das[i].Global_id)
                fmt.Println(errs)
            }
            trys = 1
            _,errs = redisync.redis_pool.HSet(keytwo,das[i].Global_id,string(b)).Result()
            for errs!=nil && trys<=Trycount {
                _,errs = redisync.redis_pool.HSet(keytwo,das[i].Global_id,string(b)).Result()
                trys++
            }
            if errs!=nil{
                fmt.Println("新增：Redis--",keytwo,"--失败---",das[i].Global_id)
                fmt.Println(errs)
            }
            todays := getTodays()
            if historys == todays && (das[i].Source_type == 4 || das[i].Source_type == 3){
                keythree := strconv.Itoa(das[i].Stu_id) + "_0_" + todays
                keyfour := strconv.Itoa(das[i].Stu_id) + "_1_" + todays
                keythreekeys := strconv.Itoa(das[i].Stu_id) + "_0_" + todays+"_keys"
                keyfourkeys := strconv.Itoa(das[i].Stu_id) + "_1_" + todays+"_keys"
                for _,ke := range redisync.redis_pool.Keys(strconv.Itoa(das[i].Stu_id) + "_0_*").Val(){
                    if ke!=keythree && ke!=keythreekeys{
                        if redisync.redis_pool.Del(ke).Val() > 0{
                            fmt.Println("删除旧Key--",ke)
                        }
                    };
                }
                for _,ke := range redisync.redis_pool.Keys(strconv.Itoa(das[i].Stu_id) + "_1_*").Val(){
                    if ke!=keyfour && ke!=keyfourkeys{
                        if redisync.redis_pool.Del(ke).Val() > 0{
                            fmt.Println("删除旧Key--",ke)
                        }
                    }
                }
                if das[i].Source_type == 4{  
                    trys = 1
                    _,errs = redisync.redis_pool.HSet(keythree,das[i].Global_id,string(b)).Result()
                    redisync.redis_pool.RPush(keythreekeys,das[i].Global_id)
                    for errs!=nil && trys<=Trycount {
                        _,errs = redisync.redis_pool.HSet(keythree,das[i].Global_id,string(b)).Result()
                        trys++
                    }
                    if errs!=nil{
                        fmt.Println("新增：Redis--",keythree,"--失败---",das[i].Global_id)
                        fmt.Println(errs)
                    }
                }
                if das[i].Source_type == 3{
                    trys = 1
                    _,errs = redisync.redis_pool.HSet(keyfour,das[i].Global_id,string(b)).Result()
                    redisync.redis_pool.RPush(keyfourkeys,das[i].Global_id)
                    for errs!=nil && trys<=Trycount {
                        _,errs = redisync.redis_pool.HSet(keyfour,das[i].Global_id,string(b)).Result()
                        trys++
                    }
                    if errs!=nil{
                        fmt.Println("新增：Redis--",keyfour,"--失败---",das[i].Global_id)
                        fmt.Println(errs)
                    }
                }
            }
       }
    }
    //pl.Exec()
}

func UpdateToRedis(das []DataSingle){
    redisync.mux.Lock()
    defer redisync.mux.Unlock()
    for i:=0;i<len(das);i++{
        keyone := "all_"+strconv.Itoa(das[i].Stu_id)
        UpdateSingle(das[i],keyone)
        historys := time.Unix(int64(das[i].Submit_time),0).Format("20060102")
        keytwo := strconv.Itoa(das[i].Stu_id) + "_" + historys
        _,erro := redisync.redis_pool.HExists(keytwo,das[i].Global_id).Result()
        trys := 1
        for erro!=nil && trys <=Dayscount{
            nn := strconv.Itoa(24*trys)
            d, _ := time.ParseDuration("-"+nn+"h")
            historys = time.Unix(int64(das[i].Submit_time),0).Add(d).Format("20060102")
            keytwo = strconv.Itoa(das[i].Stu_id) + "_" + historys
            _,erro = redisync.redis_pool.HExists(keytwo,das[i].Global_id).Result()
            trys++
        }
        if erro==nil{
            UpdateSingle(das[i],keytwo)
        }
        todays := getTodays()
        historys = time.Unix(int64(das[i].Submit_time),0).Format("20060102")
        if historys == todays {
            if das[i].Source_type==4{
                ket := strconv.Itoa(das[i].Stu_id) + "_0_" + todays
                UpdateSingle(das[i],ket)
            }
            if das[i].Source_type==3{
                ket := strconv.Itoa(das[i].Stu_id) + "_1_" + todays
                UpdateSingle(das[i],ket)
            }
        }
    }
}

func UpdateSingle(rr DataSingle,keyx string){
        cs,errs := redisync.redis_pool.HGet(keyx,rr.Global_id).Result()
        trys := 1
        for errs != nil && trys <=Trycount{
            cs,errs = redisync.redis_pool.HGet(keyx,rr.Global_id).Result()
            trys++
        }
        if errs != nil{
            fmt.Println("更新：Redis--",keyx,"--失败---",rr.Global_id,"---原因是--未能获取到field")
        }else{
            var stb DataS
            err1 := json.Unmarshal([]byte(cs), &stb)
            if err1 != nil {
             fmt.Println("更新：Redis--",keyx,"--失败---",rr.Global_id,"---原因是--json未能decode")
            } else {
                if rr.Source_type==8{
                    stb.Stu_score = rr.Stu_score
                    stb.Question_score = rr.Question_score
                }
                stb.Score = rr.Score
                stb.Used_time = rr.Used_time
                stb.Submit_time = rr.Submit_time
                stb.Is_submit = rr.Is_submit
                b, err2 := json.Marshal(stb)
                if err2 != nil {
                   fmt.Println("更新：Redis--",keyx,"--失败---",rr.Global_id,"---原因是--json未能encode")
                }else{
                   trys = 1
                   _,errsx := redisync.redis_pool.HSet(keyx,rr.Global_id,string(b)).Result()
                   for errsx!=nil && trys <=Trycount{
                        _,errsx = redisync.redis_pool.HSet(keyx,rr.Global_id,string(b)).Result()
                        trys++
                   }
                   if  errsx!=nil{
                      fmt.Println("更新：Redis--",keyx,"--失败---",rr.Global_id,"---原因是--未能重新塞入")
                   }
                }
            }
        }
}

func getTodays() string{
    nn := strconv.Itoa(24*18)
    dx, _ := time.ParseDuration("-"+nn+"h")
    todays := time.Now().Add(dx).Format("20060102")
    return todays
}