/*
Author : Phani Teja Kesha
 */

package main

import (
	"os"
	"fmt"
	"strconv"
	"log"
	"encoding/json"
	"container/list"
	"time"
	"sync"
)

//Structure for worker sending the message to the coordinator
type Message struct {
	T_id int;
	Partial_Sum int64;
	Prefix string;
	Suffix string;
	Error string;
}

//Structure for coordinator sending the message to the worker
type Input struct {
	T_id int;
	Fil_Name string;
	StartPos int64;
	EndPos   int64;
}

//function to check for any errors and log it
func check(e error){
	if e!=nil{
		panic(e)
	}
}

func main(){
	start := time.Now()
	Arguments := os.Args[1:]
	File_Name := Arguments[1]
	Number_Of_Workers := Arguments[0]
	fmt.Println("The File which needs to be added is:",File_Name)
	fmt.Println("The Number of Workers to be spawned is:",Number_Of_Workers)

	// Calling the coordinator
	Total_sum := Coordinator(File_Name,Number_Of_Workers);

	// Printing the sum
	fmt.Println("Total sum of the integers in the file is:",Total_sum);
	elapsed := time.Since(start)
	fmt.Println("Time taken for the program to complete:",elapsed)
}

func Coordinator( FileName string ,NumberOfWorkers string) int64 {


	// Converting the Number of Workers into a integer Number
	NoOfWorkers, err := strconv.Atoi(NumberOfWorkers)
	if err != nil {
		log.Fatal(err)
	}

	wg := new(sync.WaitGroup)
	wg.Add(NoOfWorkers)


	// To assign a variable to file for accessing it
	File_Open, err := os.Open(FileName)
	if err != nil {
		log.Fatal(err)
	}

	// To get the statistics of the file
	File_Stats, err := File_Open.Stat()
	if err != nil {
		log.Fatal(err)
	}
	Size_Of_File := File_Stats.Size()

	// To break the file into equal chunks of size
	Size_For_Worker := Size_Of_File / int64(NoOfWorkers)
	Size_For_Lastworker := Size_Of_File - Size_For_Worker*(int64(NoOfWorkers)-1)

	// Spawning the required number of workers
	c := make(chan []byte) //channel for communication between worker and coordinator
	l := list.New()        //List for storing the results from the worker
	var x []byte

	for i := 0; i < NoOfWorkers; i++ {

		//To handle the last worker which can get a larger chunk to sum
		if i == NoOfWorkers-1 {
			//Creating a input structure to pass into a json format
			m := Input{i, FileName, Size_For_Worker * int64(i), Size_For_Worker*int64(i) + Size_For_Lastworker}
			//creation of json message to send to the worker
			json_send, err := json.Marshal(m)
			check(err)
			//Initiliazing the worker thread with the JSON Message m,channel c,output list l
			go Worker(json_send, c,wg)
			//recieve the message from worker that the thread is completed in channel x
			break
		}
		//Case for every worker
		m := Input{i, FileName, Size_For_Worker * int64(i), Size_For_Worker*int64(i) + Size_For_Worker}
		json_send, err := json.Marshal(m)
		check(err)
		go Worker(json_send, c,wg)
	}
	for k:=0 ; k <NoOfWorkers ; k++ {
		x = <-c
		(l).PushBack(string(x))
	}

	var sumOfPartials int64
	count := 0
	li := list.New()

	wg.Wait()
	//To sort the list by the thread id numbers
	for i:=0;i<NoOfWorkers;i++{
		for temp := l.Front(); temp != nil; temp = temp.Next() {
			m := &Message{}
			json.Unmarshal([]byte(temp.Value.(string)), m)
			if m.T_id==i{
				(li).PushBack(temp.Value)
				break
			}
		}
	}

	//To handle the messages given by the workers
	for temp := li.Front(); temp != nil; temp = temp.Next() {
		m := &Message{}
		json.Unmarshal([]byte(temp.Value.(string)), m)
		//If the chunk has no prefix and suffix ,it needs to be handled
		if count==0{
			count=1
			sumOfPartials+=m.Partial_Sum
			if NoOfWorkers==1{
				SufFirst, err := strconv.Atoi(m.Suffix)
				if err != nil {
					log.Fatal(err)
				}
				sumOfPartials+=int64(SufFirst)
			}
			if m.Prefix==""{
				continue
			}
			if m.Prefix!=""{
				PreFirst, err := strconv.Atoi(m.Prefix)
				if err != nil {
					log.Fatal(err)
				}
				sumOfPartials+=int64(PreFirst)
				continue
			}
		}
		sumOfPartials+=m.Partial_Sum
		n:=&Message{}
		json.Unmarshal([]byte(temp.Prev().Value.(string)), n)
		if n.Error=="" {
			split_num := n.Suffix + m.Prefix
			pre1, err := strconv.Atoi(split_num)
			if err != nil {
				log.Fatal(err)
			}
			sumOfPartials += int64(pre1)
		}
		if m.T_id==NoOfWorkers-1{
			if m.Suffix==""{
				break
			}
			if m.Suffix!=""{
				suffLast, err := strconv.Atoi(m.Suffix)
				if err != nil {
					log.Fatal(err)
				}
				sumOfPartials+=int64(suffLast)
				break
			}
		}

		if m.Error!=""{
			fmt.Println("Error Handled")
			f,err:= os.Open(FileName)
			if err != nil {
				log.Fatal(err)
			}
			f.Seek(0,0)
			//Reading the bytes required from the seek position
			b1 := make([]byte,Size_Of_File)
			f.Read(b1)
			nums:=string(b1)
			le:=len(nums)
			if nums[le-1:le]=="\n"{
				nums=nums[0:le-1]
			}
			var flag int
			flag=0
			for k:=0;k<len(nums);k++{
				if string(nums[k])==" "{
					flag=1
				}
			}

			if flag==0{
				ans, err := strconv.Atoi(nums)
				if err!=nil{
					log.Fatal(err)
				}
				return int64(ans)
			}
			if flag==1{
				f, err := os.Open(FileName)
				if err != nil {
					log.Fatal(err)
				}
				f.Seek(0, 0)
				File_Stats, err := f.Stat()
				if err != nil {
					log.Fatal(err)
				}
				Size_Of_File := File_Stats.Size()

				b1 := make([]byte, Size_Of_File)
				f.Read(b1)
				nums := string(b1)
				le := len(nums)
				if nums[le-1:le] == "\n" {
					nums = nums[0:le-1]
				}

				if string(nums[0])==" "{
					nums=nums[1:]
				}
				le = len(nums)
				var arr []int64
				var tempr string
				var x int
				for z:=0;z<len(nums);z++{
					if string(nums[z])==" "{
						x=z
						ans, err := strconv.Atoi(tempr)
						if err!=nil{
							log.Fatal(err)
						}
						arr=append(arr,int64(ans))
						tempr=""
						continue
					}
					tempr=tempr+string(nums[z])
				}
				ans, err := strconv.Atoi(string(nums[x+1:]))
				if err!=nil{
					log.Fatal(err)
				}
				arr=append(arr,int64(ans))
				var sum int64
				for k:=0;k<len(arr);k++{
					sum+=arr[k]
				}
				return sum
			}
			if m.T_id!=0 {
				p := &Message{}
				json.Unmarshal([]byte(temp.Prev().Value.(string)), p)

				s := &Message{}
				json.Unmarshal([]byte(temp.Next().Value.(string)), s)
				split := p.Suffix + m.Error + s.Prefix
				split_i, err := strconv.Atoi(split)
				if err != nil {
					log.Fatal(err)
				}
				sumOfPartials += int64(split_i)
				continue
			}
		}
	}
	return sumOfPartials
}

func Worker (b []byte,c chan []byte,wg *sync.WaitGroup){

	//Decoding the json message recieved from the coordinator
	var inp Input;
	err:=json.Unmarshal(b,&inp)
	check(err)

	//Opening and reading a file to access its elements
	f, err := os.Open(inp.Fil_Name)
	if err != nil {
		log.Fatal(err)
	}

	//send the file seek pointer to the start position of the file
	if inp.T_id==0{
		frstchar := make([]byte, 1)
		f.Read(frstchar)
		fchar:=string(frstchar)
		if fchar==" " {
			inp.StartPos = inp.StartPos + 1
		}
	}
	f.Seek(inp.StartPos,0)

	//Reading the bytes required from the seek position

	b1 := make([]byte, inp.EndPos - inp.StartPos)
	f.Read(b1)
	nums:=string(b1)
	le:=len(nums)
	if nums[le-1:le]=="\n"{
		nums=nums[0:le-1]
	}
	//The chunk does not have a prefix and suffix
	var flag int
	flag=0
	for k:=0;k<len(nums);k++{
		if string(nums[k])==" "{
			flag=1
		}
	}

	if flag==0{

		//Creating a structure to send back to the coordinator
		m:=Message{inp.T_id,0,"","",nums}
		//Encoding the message m to send back
		out,_ := json.Marshal(m)

		//out is sent back to the coordinator to intimate that the work assigned for the given thread is completed by channels
		c <- out
	}

	if flag==1{

	//For finding the prefix of the given chunk of file
	var x int
	for i := 0; i<len(nums); i++{
	if string(nums[i])==" " {
	x = i
	break
	}
	}
	pre_fix :=string(nums[:x])

	//For finding the suffix of the given chunk of file
	var y int
	for i := 0; i<len(nums); i++{
	if string(nums[i])==" " {
	y = i
	}

	}
	suf_fix :=string(nums[y+1:])

	//changing all the numerical elements in the file chunk into a array of integers
	var temp string
	var numb []int64
	for j:= x+1; j<=y; j++ {
	if string((nums[j]))==" "{
	nump, err := strconv.Atoi(temp)
	if err!=nil{
	log.Fatal(err)
	}
	numb = append(numb, int64(nump))
	temp = ""
	continue
	}
	temp = temp+string((nums[j]))
	}

	//Summation of all the integers in the list
	var par_sum int64
	for k := 0;k<len(numb); k++{
	par_sum+=numb[k]
	}

	//Creating a structure to send back to the coordinator
	m := Message{inp.T_id, par_sum, string(pre_fix), string(suf_fix), ""}
	fmt.Println("Thread id:",m.T_id,"Partial Sum:",m.Partial_Sum,"Prefix:",m.Prefix,"Suffix:",m.Suffix,"Error:",m.Error)
	//Encoding the message m to send back
	out, _ := json.Marshal(m)
	//out is sent back to the coordinator to intimate that the work assigned for the given thread is completed using channels
	c <- out
	}
	wg.Done()
}
