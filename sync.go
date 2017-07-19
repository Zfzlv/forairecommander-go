package main

import (
    _ "github.com/go-sql-driver/mysql"
    "github.com/go-xorm/xorm"
    "fmt"
    //"log"
    "time"
    //"database/sql"
    "flag"
    "github.com/satori/go.uuid"
    //"reflect"
    "strconv"
    "runtime"
    "runtime/debug"
    "sync"
    "github.com/go-redis/redis"
    "encoding/json"
    "os"
    //"io"
)
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
type Exercise struct {  
    Id  int  `xorm:"int(10) pk autoincr notnull unique 'id'"`
    Stu_id  int  `xorm:"int(11) index 'stu_id'"`
    Type    int  `xorm:"int(11) 'type'"`
    Source_id   string   `xorm:"varchar(255) 'source_id'"`
    Subject int  `xorm:"tinyint(4) 'subject'"`
    Submit_time int  `xorm:"int(11) 'submit_time'"`
    Used_time   int  `xorm:"int(11) 'used_time'"`
    Status  int  `xorm:"tinyint(3) 'status'"`
    Source  int  `xorm:"tinyint(3) 'source'"`
    Correct_teacher_id  int  `xorm:"int(11) 'correct_teacher_id'"`
    Created_at  time.Time  `xorm:"timestamp 'created_at'"`
    Updated_at  time.Time  `xorm:"timestamp 'updated_at'"`
    Correct_over_time   int  `xorm:"int(11) 'correct_over_time'"`
}  
type ExerciseQuestion struct {  
    Id    int    `xorm:"int(10) pk autoincr notnull unique 'id'"`
    Exercise_id    int    `xorm:"int(11) index(exercise_question_exercise_id_stu_id_index) 'exercise_id'"`
    Number    int    `xorm:"int(11) 'number'"`
    Question_id    int    `xorm:"int(11) 'question_id'"`
    Answer    string    `xorm:"text 'answer'"`
    Stu_answer    string    `xorm:"text index(stu_ans_updated_index)  'stu_answer'"`
    Origin_stu_answer    string    `xorm:"text 'origin_stu_answer'"`
    Score    int    `xorm:"int(10)  index(e_q_stu_submit_sco_subject) 'score'"`
    Difficulty    int    `xorm:"int(11) 'difficulty'"`
    Used_time    int    `xorm:"int(11) 'used_time'"`
    Submit_time    int    `xorm:"int(11) index(e_q_stu_submit_sco_subject) 'submit_time'"`
    Push_time    int    `xorm:"int(11) 'push_time'"`
    Status    int    `xorm:"int(11) 'status'"`
    Subject    int    `xorm:"int(11)  index(e_q_stu_submit_sco_subject) 'subject'"`
    Comment    string    `xorm:"text 'comment'"`
    Created_at    time.Time    `xorm:"timestamp 'created_at'"`
    Updated_at    time.Time    `xorm:"timestamp index(stu_ans_updated_index) 'updated_at'"`
    Question_type    int    `xorm:"int(11) 'question_type'"`
    Stu_id    int    `xorm:"int(11) index(exercise_question_exercise_id_stu_id_index,e_q_stu_submit_sco_subject) 'stu_id'"`
    Points    string    `xorm:"varchar(255) 'points'"`
} 
type StudentHomework struct {  
    Id    int    `xorm:"int(10) pk autoincr notnull unique 'id'"`
    Homework_id    int    `xorm:"int(11) index(student_homework_homework_id_index,student_homework_stu_id_homework_id_unique) 'homework_id'"`
    Stu_id    int    `xorm:"int(11) index(student_homework_stu_id_homework_id_unique) 'stu_id'"`
    Subject    int    `xorm:"tinyint(3) 'subject'"`
    Class_id    int    `xorm:"int(11) 'class_id'"`
    Status    int    `xorm:"tinyint(3) 'status'"`
    Score    int    `xorm:"tinyint(3) 'score'"`
    Correct_number    int    `xorm:"tinyint(4) 'correct_number'"`
    Used_time    int    `xorm:"int(11) 'used_time'"`
    Submit_time    int    `xorm:"int(11) 'submit_time'"`
    Source    int    `xorm:"tinyint(11) 'source'"`
    Is_claimed    int    `xorm:"int(11) 'is_claimed'"`
    Correct_over_time    int    `xorm:"int(11) 'correct_over_time'"`
    Created_at    time.Time    `xorm:"timestamp 'created_at'"`
    Updated_at    time.Time    `xorm:"timestamp 'updated_at'"`
} 
type StudentHomeworkQuestion struct {  
    Id    int    `xorm:"int(10)  pk autoincr notnull unique 'id'"`
    Homework_id    int    `xorm:"int(11) index(student_homework_question_homework_id_index,student_homework_question_stu_id_homework_id_number_unique) 'homework_id'"`
    Subject    int    `xorm:"int(11) 'subject'"`
    Stu_id    int    `xorm:"int(11) index(student_homework_question_stu_id_homework_id_number_unique) 'stu_id'"`
    Class_id    int    `xorm:"int(11) 'class_id'"`
    Number    int    `xorm:"int(11) index(student_homework_question_stu_id_homework_id_number_unique) 'number'"`
    Question_id    int    `xorm:"int(11) index 'question_id'"`
    Question_type    int    `xorm:"int(11) 'question_type'"`
    Stu_answer    string    `xorm:"text index(stu_answer_index,stu_ans_updated_index,stu_answer_updated_index) 'stu_answer'"`
    Origin_stu_answer    string    `xorm:"text 'origin_stu_answer'"`
    Score    int    `xorm:"tinyint(3) index 'score'"`
    Status    int    `xorm:"int(11) 'status'"`
    Used_time    int    `xorm:"int(11) 'used_time'"`
    Submit_time    int    `xorm:"int(11) 'submit_time'"`
    Push_time    int    `xorm:"int(11) 'push_time'"`
    Comment    string    `xorm:"varchar(255) 'comment'"`
    Postil    string    `xorm:"varchar(255) 'postil'"`
    Created_at    time.Time    `xorm:"timestamp 'created_at'"`
    Updated_at    time.Time    `xorm:"timestamp index(updated_at_index,stu_ans_updated_index,stu_answer_updated_index) 'updated_at'"`
}
type StudentExam struct {  
    Id    int    `xorm:"int(10)  pk autoincr notnull unique  'id'"`
    Class_id    int    `xorm:"int(11) 'class_id'"`
    Stu_id    int    `xorm:"int(11) index(student_exam_stu_id_exam_id_unique) 'stu_id'"`
    Exam_id    int    `xorm:"int(11) index(student_exam_stu_id_exam_id_unique) 'exam_id'"`
    Subject    int    `xorm:"tinyint(3) 'subject'"`
    Status    int    `xorm:"tinyint(3) 'status'"`
    Start_time    int    `xorm:"int(11) 'start_time'"`
    Stop_time    int    `xorm:"int(11) 'stop_time'"`
    Upload_time    int    `xorm:"int(11) 'upload_time'"`
    Score    int    `xorm:"int(11) 'score'"`
    Used_time    int    `xorm:"int(11) 'used_time'"`
    Created_at    time.Time    `xorm:"timestamp 'created_at'"`
    Updated_at    time.Time    `xorm:"timestamp 'updated_at'"`
}
type StudentExamQuestion struct {  
    Id    int    `xorm:"int(10)  pk autoincr notnull unique  'id'"`
    Stu_id    int    `xorm:"int(11) index(student_exam_question_stu_id_exam_id_number_unique) 'stu_id'"`
    Exam_id    int    `xorm:"int(11) index(student_exam_question_stu_id_exam_id_number_unique,student_exam_question_exam_id_teacher_id_index) 'exam_id'"`
    Class_id    int    `xorm:"int(11) 'class_id'"`
    Number    int    `xorm:"int(11) index(student_exam_question_stu_id_exam_id_number_unique) 'number'"`
    Question_id    int    `xorm:"int(11) index 'question_id'"`
    Question_score    int    `xorm:"int(11) 'question_score'"`
    Question_type    int    `xorm:"tinyint(3) 'question_type'"`
    Stu_answer    string    `xorm:"varchar(255) index(stu_ans_updated_index,stu_answer_index) 'stu_answer'"`
    Origin_stu_answer    string    `xorm:"varchar(255) 'origin_stu_answer'"`
    Teacher_id    int    `xorm:"int(11) index(student_exam_question_exam_id_teacher_id_index) 'teacher_id'"`
    Score    int    `xorm:"int(11) 'score'"`
    Submit_time    int    `xorm:"int(11) 'submit_time'"`
    Used_time    int    `xorm:"int(11) 'used_time'"`
    Status    int    `xorm:"int(11) 'status'"`
    Created_at    time.Time    `xorm:"timestamp 'created_at'"`
    Updated_at    time.Time    `xorm:"timestamp index(updated_at_index,stu_ans_updated_index) 'updated_at'"`
}
type IdRange struct{
    Min int 
    Max int
}
type Data struct{
    Source_type int 
    Source_id string
    Subject int
    Zhu_use int
    Zhu_sub int
    Number int
    Question_id int
    Question_type int
    Stu_answer string
    Score int
    Stu_score string
    Question_score int
    Used_time int
    Submit_time int
    Stu_id int
    Original_id string
    Zhu_Score int
    Zhu_Stscore string
    Created_at time.Time
    Is_submit int
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

type SafeRedis struct {
    redis_pool   *redis.Client
    mux sync.Mutex
}
var en_origin *xorm.Engine
var en_local *xorm.Engine
const Counts = 100
//var waitgroup sync.WaitGroup
//var ch = make(chan []Data,4000000)
//var chforRedis = make(chan []DataSingle,4000000)
var t1 time.Time
var redisync SafeRedis
const Trycount = 5
const Dayscount = 5
var ridge,_ = time.Parse("2006-01-02","2017-07-10")
func init() {  
    var err error  
    t1 = time.Now()
    fmt.Println("-修复脚本开始运行-",time.Now().String())
    en_origin, err = xorm.NewEngine("mysql", "rd:DSykz6a7Tu8MnN8G@tcp(rr-bp156ya5cm9d985sdo.mysql.rds.aliyuncs.com:3306)/tangseng?charset=utf8")
    en_origin.SetMaxIdleConns(5)
    if err != nil {
        panic(err.Error())
    }
    fmt.Println("--已连上AI库---")
    //defer en_origin.Close()
    en_local, err = xorm.NewEngine("mysql", "wenba:KDAN82aw5g2XDNK4SQ6RsuEc4pl9DtBH@tcp(rm-bp1e92v83gxr464y6o.mysql.rds.aliyuncs.com:3306)/zujuan?charset=utf8")
    en_local.SetMaxIdleConns(5)
    if err != nil {
        panic(err.Error())
    }
    fmt.Println("--已连上本地库---") 
   //defer en_local.Close()
    //en_origin.ShowSQL(true)  
    //en_local.ShowSQL(true) 
/*
    if err = en_local.Sync(
        new(Tangseng),
        new(TangsengQuestions),
        new(TangsengMiddle)); err != nil {  
        log.Fatalf("Fail to sync struct to  table schema : %v", err)  
    } else {  
        fmt.Println("Succ sync struct to table schema")  
    }  
*/
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
  
/*
func getTangseng_middle() (middles []tangseng_middle, err error) {  
    ////////get person from DB  
    middles = make([]tangseng_middle, 0)  
  
    if err = en_local.Find(&middles); err != nil {  
        return nil, err  
    }  
  
    fmt.Printf("Succ to get middles number : %d\n", len(middles))  
    for i, d := range middles {  
        fmt.Printf("DataIndex : %d        DataContent : %#v\n", i, d)  
    }  
    return middles, nil  
} 
  */
func main() {  
    //fmt.Println(getTodays())
    //fmt.Println(reflect.TypeOf(time.Unix(1499927308, 0).Format("20060102  03:04:05")))
   start := flag.String("start", "", "开始时间")
   end := flag.String("end", "", "结束时间")
   stuid := flag.String("stuid", "", "学生ID")
   flag.Parse()
   var s,e time.Time
   var errs error
   if *start!=""{
     s, errs = time.Parse("2006-01-02", *start)  
     if errs != nil{
        fmt.Println("开始时间格式错误！！正确格式应该为--start=\"2017-03-16\"")
        return  
     }
   }
   if *end!=""{
     e, errs = time.Parse("2006-01-02", *end)  
     if errs != nil{
        fmt.Println("结束时间格式错误！！正确格式应该为--end=\"2017-03-16\"")
        return  
     }
   }
    //fmt.Println(s.Format("20060102")=="00010101")
   if *start!="" && *end!="" && s.After(e){
     fmt.Println("结束时间应该在开始时间后面！！")
     return  
   }
   fmt.Println("--即将开始同步修复---")
   /*fmt.Println(strconv.FormatInt(s.Unix(),10))
   fmt.Println(time.Unix(int64(1489622400),0).Format("20060102"))*/
   DiffDB(*start,*end,*stuid)
   /*redisync.redis_pool.HSet("all","1042_2989_2_74254020_2_6_4","qw")
   redisync.redis_pool.HSet("all","1042_2989_2_73935339_1_6_4","qw")
   redisync.redis_pool.HSet("all","1042_3000_2_73937295_1_0_8","qw")*/
   //fmt.Println(reflect.TypeOf(strconv.Itoa(tt.Min)))
    //waitgroup.Wait()
    t2:=time.Now()
    d:=t2.Sub(t1)
    fmt.Println("--本次同步修复完成---",time.Now().String())
    fmt.Println("--共花费时长为---",d)

}  

func GetIDRange(tablename,start,end string) IdRange{
    var tmp IdRange
    var sqls = "select min(id) as Min,max(id) as Max from "+ tablename + " where 1=1 "
    if start != "" {
       sqls += " and created_at >= \"" + start + "\""
    }
    if end != "" {
       sqls += " and created_at <= \"" + end + "\""
    }

    if has,err := en_origin.SQL(sqls).Get(&tmp); err != nil || has == false { 
            fmt.Println("***Error***Get***MiMax******"+tablename+"**begin from***"+time.Now().String())
    }

    return tmp
}

func GetUUID() uuid.UUID{
   return uuid.NewV4()
}
func gc(){
    debug.FreeOSMemory()
    runtime.GC()
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

func InsertOrUpdateDb(datas []Data){
         //datas := <- ch
         var questions []TangsengQuestions
         var middles []TangsengMiddle
         var dataforredis []DataSingle
         var dataforupdateredis []DataSingle
         for i:=len(datas)-1;i>=0;i--{
            var tangs []Tangseng
            var fla int = 0
            var fla_q int = 0
            if datas[i].Source_type == 0 {
                datas[i].Source_type = 4
            }else if datas[i].Source_type == 1{
                datas[i].Source_type = 3
            }
            var global_id = strconv.Itoa(datas[i].Stu_id)+"_"+datas[i].Source_id+"_"+strconv.Itoa(datas[i].Source_type)+"_"+strconv.Itoa(datas[i].Question_id)+"_"+strconv.Itoa(datas[i].Question_type)+"_"+strconv.Itoa(datas[i].Subject)
            tmp_q := TangsengQuestions{Id:global_id}
            has,err := en_local.Get(&tmp_q);
            tmp_t := Tangseng{Original_id:datas[i].Original_id}
            has_1,err_1 := en_local.Get(&tmp_t);
            if err == nil && has == true{ 
                if tmp_q.Question_type != datas[i].Question_type{
                    fla_q=1
                   tmp_q.Question_type = datas[i].Question_type
                }
                if tmp_q.Score != datas[i].Score{
                    fla_q=1
                    tmp_q.Score = datas[i].Score
                }
                if datas[i].Source_type==8 && tmp_q.Stu_score != datas[i].Stu_score{
                    fla_q=1
                    tmp_q.Stu_score = datas[i].Stu_score
                }
                if datas[i].Source_type==8 && tmp_q.Question_score != datas[i].Question_score{
                    fla_q=1
                    tmp_q.Question_score = datas[i].Question_score
                }
                if tmp_q.Used_time != datas[i].Used_time{
                    fla_q=1
                    tmp_q.Used_time = datas[i].Used_time
                } 
                if tmp_q.Stu_answer != datas[i].Stu_answer{
                    fla_q=1
                    tmp_q.Stu_answer = datas[i].Stu_answer
                } 
                if tmp_q.Submit_time != datas[i].Submit_time{
                    fla_q=1
                    tmp_q.Submit_time = datas[i].Submit_time
                }
                if tmp_q.Is_submit != datas[i].Is_submit{
                    fla_q=1
                    tmp_q.Is_submit = datas[i].Is_submit
                }
            }else{
                tmp_q = TangsengQuestions{
                Id: global_id,
                Number:datas[i].Number,
                Question_id:datas[i].Question_id,
                Question_type:datas[i].Question_type,
                Stu_answer:datas[i].Stu_answer,
                Score:datas[i].Score,
                Stu_score:datas[i].Stu_score,
                Question_score:datas[i].Question_score,
                Used_time:datas[i].Used_time,
                Submit_time:datas[i].Submit_time,
                Created_at:datas[i].Created_at,
                Status:1,
                Is_submit:datas[i].Is_submit}
            }
            var types=1;
            if datas[i].Score > 0{
                    types=2;
            }
            if err_1 != nil || has_1 == false{
                uid := GetUUID()
                tmp_t = Tangseng{
                    Id:uid.String(),
                    Types:types,
                    Stu_id:datas[i].Stu_id,
                    Source_type:datas[i].Source_type,
                    Source_id:datas[i].Source_id,
                    Subject:datas[i].Subject,
                    Stu_score_percent:datas[i].Zhu_Score,
                    Stu_score:datas[i].Zhu_Stscore,
                    Used_time:datas[i].Zhu_use,
                    Submit_time:datas[i].Zhu_sub,
                    Status:1,
                    Original_id:datas[i].Original_id}
                tangs = append(tangs,tmp_t)
                back := createTangseng(tangs)
                for back ==0{
                   tmp_t = Tangseng{Original_id:datas[i].Original_id}
                   has_1,err_1 = en_local.Get(&tmp_t); 
                   if err_1 != nil || has_1 == false{
                     back = createTangseng(tangs)
                   }else{
                     back=1
                   }
                } 
            }
                if types==2 {
                    if tmp_t.Types != types{
                        fla = 1
                    }
                    tmp_t.Types = types
                }
                fff, err2 := strconv.ParseFloat(datas[i].Zhu_Stscore, 32)
                if err2 != nil{ 
                }
                if fff>0{
                    if tmp_t.Stu_score != datas[i].Zhu_Stscore{
                        fla = 1
                    }
                    tmp_t.Stu_score = datas[i].Zhu_Stscore
                }
                if tmp_t.Used_time != datas[i].Zhu_use{
                    fla = 1
                }
                if  datas[i].Source_type==8 && datas[i].Zhu_sub > tmp_t.Submit_time {
                    fla = 1
                    tmp_t.Submit_time = datas[i].Zhu_sub
                }else{
                    if datas[i].Zhu_sub != tmp_t.Submit_time{
                        fla = 1
                    }
                    tmp_t.Submit_time = datas[i].Zhu_sub 
                }
                //tmp_t.Source_type = datas[i].Source_type
                //tmp_t.Source_id = datas[i].Source_id
                //tmp_t.Subject = datas[i].Subject
                //tmp_t.Stu_score_percent = datas[i].Zhu_Score
                tmp_t.Used_time = datas[i].Zhu_use
                if fla == 1{
                    tmp_t.Status = 2
                    en_local.Id(tmp_t.Id).Cols("Types","Stu_score","Used_time","Submit_time","Status").Update(&tmp_t)
                }
    
            if err != nil || has == false{
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
                  if tmp_r.Submit_time == 0{
                     tmp_r.Submit_time = int(datas[i].Created_at.Unix())
                  }
                  questions = append(questions,tmp_q)
                  dataforredis = append(dataforredis,tmp_r)
                  tmp_m := TangsengMiddle{
                    Tangseng_id:tmp_t.Id,
                    Global_id:global_id}
                  middles = append(middles,tmp_m)
            }else if fla_q==1{
                tmp_q.Status=2
                if datas[i].Source_type==8{
                    en_local.Id(global_id).Cols("Question_type","Stu_answer","Score","Stu_score","Question_score","Used_time","Submit_time","Status").Update(&tmp_q)
                }else{
                    en_local.Id(global_id).Cols("Question_type","Stu_answer","Score","Used_time","Submit_time","Status").Update(&tmp_q)
                }
            } 
            if fla==1 || fla_q==1{
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
                if tmp_u.Submit_time == 0{
                     tmp_u.Submit_time = int(datas[i].Created_at.Unix())
                }
                dataforupdateredis = append(dataforupdateredis,tmp_u)
            }
         }
         if len(questions) > 0 {
             createTangsengQuestion(questions) 
             /*waitgroup.Add(1)
             chforRedis <- dataforredis
             go func(){
                SetToRedis(chforRedis)
             }()*/
             SetToRedis(dataforredis)
         } 
         if len(middles) > 0 {
             createTangsengMiddle(middles)
         }
         if  len(dataforupdateredis) > 0 {
             /*waitgroup.Add(1)
             chforUpdateRedis <- dataforupdateredis
             go func(){
                UpdateToRedis(chforUpdateRedis)
             }()*/
             UpdateToRedis(dataforupdateredis)
         }
         //waitgroup.Done()
}

func DiffDB(s,e,stuid string){
    var start,end time.Time
    if s==""{
        s="2016-08-30"  
    }
    if e==""{
        e = time.Now().Format("20060102") 
    }
    start, _ = time.Parse("2006-01-02", s) 
    end, _ = time.Parse("2006-01-02", e)
    datas := make([]Data,0)
    for end.After(start){
        sqls := ""
        start1 := getBeforeDay(end)
        se := end.Format("20060102")
        ss := start1.Format("20060102")
        if ridge.After(start1){
            sqls = "select a.number as Number,a.question_id as Question_id,a.question_type as Question_type,a.stu_answer as Stu_answer,a.score as Score,\"0\" as Stu_score,0 as Question_score,a.used_time as Used_time,a.submit_time as Submit_time,b.type as Source_type,a.exercise_id as Source_id,b.subject as Subject,b.used_time as Zhu_use,b.submit_time as Zhu_sub,b.stu_id as Stu_id,concat(\"exercise_\",b.id) as Original_id,0 as Zhu_Score,\"0\" as Zhu_Stscore,a.created_at as Created_at,a.status as Is_submit from exercise_question a,exercise b where a.exercise_id = b.id and ((a.created_at >= \""+ss+"\" and a.created_at <= \""+se+"\") or (a.submit_time >= "+strconv.FormatInt(start1.Unix()-28800,10)+" and a.submit_time < "+strconv.FormatInt(end.Unix()-28800,10)+"))"
            sqls += " union select a.number as Number,a.question_id as Question_id,a.question_type as Question_type,a.stu_answer as Stu_answer,a.score as Score,\"0\" as Stu_score,0 as Question_score,a.used_time as Used_time,a.submit_time as Submit_time,\"2\" as Source_type,a.homework_id as Source_id,b.subject as Subject,b.used_time as Zhu_use,b.submit_time as Zhu_sub,b.stu_id as Stu_id,concat(\"homework_\",b.id) as Original_id,0 as Zhu_Score,b.score as Zhu_Stscore,a.created_at as Created_at,a.status as Is_submit from student_homework_question a,student_homework b where a.homework_id = b.homework_id and a.stu_id = b.stu_id and ((a.created_at >= \""+ss+"\" and a.created_at <= \""+se+"\") or (a.submit_time >= "+strconv.FormatInt(start1.Unix()-28800,10)+" and a.submit_time < "+strconv.FormatInt(end.Unix()-28800,10)+"))"
            sqls += " union select a.number as Number,a.question_id as Question_id,a.question_type as Question_type,a.stu_answer as Stu_answer,(case when a.score=a.question_score then 1 when a.score > (a.question_score/2) then 2 when a.score >0 then 3 else 0 end) as Score,a.score as Stu_score,a.question_score as Question_score,a.used_time as Used_time,a.submit_time as Submit_time,\"8\" as Source_type,a.exam_id as Source_id,b.subject as Subject,b.used_time as Zhu_use,a.submit_time as Zhu_sub,a.created_at as Created_at,b.stu_id as Stu_id,concat(\"exam_\",b.id) as Original_id,0 as Zhu_Score,b.score as Zhu_Stscore,a.status as Is_submit from student_exam_question a,student_exam b where a.exam_id = b.exam_id and a.stu_id = b.stu_id and ((a.created_at >= \""+ss+"\" and a.created_at <= \""+se+"\") or (a.submit_time >= "+strconv.FormatInt(start1.Unix()-28800,10)+" and a.submit_time < "+strconv.FormatInt(end.Unix()-28800,10)+"))"
        }else{
            sqls = "select a.number as Number,a.question_id as Question_id,a.question_type as Question_type,a.stu_answer as Stu_answer,a.score as Score,\"0\" as Stu_score,0 as Question_score,a.used_time as Used_time,a.submit_time as Submit_time,b.type as Source_type,a.exercise_id as Source_id,b.subject as Subject,b.used_time as Zhu_use,b.submit_time as Zhu_sub,b.stu_id as Stu_id,concat(\"exercise_\",b.id) as Original_id,0 as Zhu_Score,\"0\" as Zhu_Stscore,a.created_at as Created_at,a.status as Is_submit from exercise_question a,exercise b where a.exercise_id = b.id and a.submit_time >= "+strconv.FormatInt(start1.Unix()-28800,10)+" and a.submit_time < "+strconv.FormatInt(end.Unix()-28800,10)
            sqls += " union select a.number as Number,a.question_id as Question_id,a.question_type as Question_type,a.stu_answer as Stu_answer,a.score as Score,\"0\" as Stu_score,0 as Question_score,a.used_time as Used_time,a.submit_time as Submit_time,\"2\" as Source_type,a.homework_id as Source_id,b.subject as Subject,b.used_time as Zhu_use,b.submit_time as Zhu_sub,b.stu_id as Stu_id,concat(\"homework_\",b.id) as Original_id,0 as Zhu_Score,b.score as Zhu_Stscore,a.created_at as Created_at,a.status as Is_submit from student_homework_question a,student_homework b where a.homework_id = b.homework_id and a.stu_id = b.stu_id and a.submit_time >= "+strconv.FormatInt(start1.Unix()-28800,10)+" and a.submit_time < "+strconv.FormatInt(end.Unix()-28800,10)
            sqls += " union select a.number as Number,a.question_id as Question_id,a.question_type as Question_type,a.stu_answer as Stu_answer,(case when a.score=a.question_score then 1 when a.score > (a.question_score/2) then 2 when a.score >0 then 3 else 0 end) as Score,a.score as Stu_score,a.question_score as Question_score,a.used_time as Used_time,a.submit_time as Submit_time,\"8\" as Source_type,a.exam_id as Source_id,b.subject as Subject,b.used_time as Zhu_use,a.submit_time as Zhu_sub,a.created_at as Created_at,b.stu_id as Stu_id,concat(\"exam_\",b.id) as Original_id,0 as Zhu_Score,b.score as Zhu_Stscore,a.status as Is_submit from student_exam_question a,student_exam b where a.exam_id = b.exam_id and a.stu_id = b.stu_id and a.submit_time >= "+strconv.FormatInt(start1.Unix()-28800,10)+" and a.submit_time < "+strconv.FormatInt(end.Unix()-28800,10)
        }
        if(stuid!=""){
            sqls = "select * from ("+sqls+")e where e.Stu_id in("+stuid+");"
        }
        c := 1
         /*var f    *os.File
        var err1   error;
       if checkFileIsExist("D:\\golang\\log.txt") {  //如果文件存在
          f, err1 = os.OpenFile("D:\\golang\\log.txt", os.O_APPEND, 0666)  //打开文件
       }else {
          f, err1 = os.Create("D:\\golang\\log.txt")  //创建文件
       }
         check(err1)
         _,err1 = io.WriteString(f, sqls+"\n") //写入文件(字符串)
         check(err1)*/
      if err := en_origin.SQL(sqls).Find(&datas);err == nil{
         //ch <- datas
         /*go InsertOrUpdateDb(ch,datas)
         go func(datas []Data) {
          InsertOrUpdateDb(ch,datas)
         }(datas)*/
         InsertOrUpdateDb(datas)
         c++
         if c%Counts==0 {
            gc()
        }
      }else{
         fmt.Println("*********Error***Get***Data***From*table*"+"**begin from***"+time.Now().String())
      }
      datas = make([]Data,0)
      //time.Sleep(1 * time.Second)
      end = start1
    }
}
func SetToRedis(das []DataSingle){
    //das := <- chforRedis
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
    //waitgroup.Done()
    //pl.Exec()
}
func UpdateToRedis(das []DataSingle){
    //das := <- chforUpdateRedis
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
    //waitgroup.Done()
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
    nn := strconv.Itoa(24*19)
    dx, _ := time.ParseDuration("-"+nn+"h")
    todays := time.Now().Add(dx).Format("20060102")
    return todays
}

func getAfterDay(s time.Time) time.Time{
    dx, _ := time.ParseDuration("24h")
    afterDay := s.Add(dx)
    return afterDay
}

func getBeforeDay(s time.Time) time.Time{
    dx, _ := time.ParseDuration("-24h")
    beforeDay := s.Add(dx)
    return beforeDay
}

func checkFileIsExist(filename string) (bool) {
 var exist = true;
 if _, err := os.Stat(filename); os.IsNotExist(err) {
  exist = false;
 }
 return exist;
}

func check(e error) {
 if e != nil {
  panic(e)
 }
}
