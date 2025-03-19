package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/jcelliott/lumber"
)

const Version = "1.0.0"

type (
	Logger interface {
		Fatal(string, ...interface{})
		Error(string, ...interface{})
		Warn(string, ...interface{})
		Info(string, ...interface{})
		Debug(string, ...interface{})
		Trace(string, ...interface{})
	}

	Driver struct{
		mutex sync.Mutex
		mutexes map[string]*sync.Mutex
		dir string
		log Logger
	}
)

type Options struct {
	Logger
}
func New(dir string , options *Options)(*Driver ,error){
	dir = filepath.Clean(dir)
	opts := Options{}
	if options != nil{
		opts = *options
	}
	if opts.Logger == nil {
		opts.Logger = lumber.NewConsoleLogger((lumber.INFO))
	}

	driver := Driver{
		dir: dir,
		mutexes: make(map[string]*sync.Mutex),
		log :opts.Logger,
	}
	if _ , err := os.Stat(dir); err == nil {
		opts.Logger.Debug("Using '%s' (databse already exists)\n",dir)
		return &driver,nil
	}

	opts.Logger.Debug("Creating the database at '%s'...\n",dir)
	return  &driver, os.MkdirAll(dir, 0755)
}

func (d *Driver) Write(collection, resource string, v interface{}) error {
	// Validate collection name
	if collection == "" {
		return fmt.Errorf("missing collection - no place to save record")
	}

	// Validate resource name
	if resource == "" {
		return fmt.Errorf("missing resource - unable to save record (no name)")
	}

	// Lock the collection mutex
	mutex := d.getOrCreateMutex(collection)
	mutex.Lock()
	defer mutex.Unlock()

	// Define paths
	dir := filepath.Join(d.dir, collection)
	finalPath := filepath.Join(dir, resource+".json")
	tmpPath := finalPath + ".tmp"

	// Ensure directory exists
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	// Marshal data to JSON
	b, err := json.MarshalIndent(v, "", "\t")
	if err != nil {
		return err
	}
	b = append(b, byte('\n'))

	// Write to temp file first
	if err := os.WriteFile(tmpPath, b, 0644); err != nil {
		return err
	}



	return os.Rename(tmpPath, finalPath)
}

func (d *Driver) Read(collection , resource string, v interface{}) error {
	if collection == ""{
		return fmt.Errorf("missing collecton - no place  to read record")
	}
	if resource == ""{
		return fmt.Errorf("missing resource - unable to read record")

	}
	record := filepath.Join(d.dir,collection,resource)
	if _ , err := stat(record) ; err!= nil{
		return err
	}
	 b, err := os.ReadFile(record+ ".json")
	 if err != nil {
		return err
	 }
	 return json.Unmarshal(b, &v)
}
func (d *Driver) ReadAll(collection string)([]string,error){
		if collection == "" {
			return nil,fmt.Errorf("missing collection - unable to read")
		}

		dir := filepath.Join(d.dir,collection)
		if _,err :=	stat(dir); err!= nil {
			return nil, err
		}
		files , _ :=os.ReadDir(dir)
		var records []string;

		for _, file := range files {
			b, err := os.ReadFile(filepath.Join(dir, file.Name()))
			if err != nil {
				return nil,err
			}
			records = append(records, string(b))
		}
		return records,nil



}

func (d *Driver) Delete(collection, resource string)error {
	path := filepath.Join(collection,resource)
	mutex := d.getOrCreateMutex(collection)
	mutex.Lock()
	defer mutex.Unlock()

	dir :=filepath.Join(d.dir, path) 
	switch fi , err := stat(dir);{
	case fi == nil , err != nil :
		return fmt.Errorf("unable to find file or directory named %q",path)
	case fi.Mode().IsDir():
		return os.RemoveAll(dir)
	
	case fi.Mode().IsRegular():
		return os.RemoveAll(dir +".json")
	
	}
	return nil


}

func (d *Driver) getOrCreateMutex(collection string) *sync.Mutex{
	d.mutex.Lock()
	defer d.mutex.Unlock()
	m,ok := d.mutexes[collection]

	if !ok {
		m = &sync.Mutex{}
		d.mutexes[collection] = m

	}
	return m
}

func stat(path string) (fi os.FileInfo, err error) {
	fi, err = os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			fi, err = os.Stat(path + ".json")
		}
	}
	return
}

type Address struct {
	City string
	State string
	Country string
	Pincode json.Number
}
type User struct {
	Name string
	Age json.Number
	Contact string
	Company string
	Address Address
}

func main()  {
	fmt.Println("Welcome to a my database")
	dir := "./"

	db,err := New(dir,nil)
	if err != nil {
		fmt.Println("Error",err)
	}

	employees := []User{
		{"Ronaldo", "23","22748292","Google",Address{"Kharghar","Navi mumbai","India","410210"}},
		{"Pratim", "45","2903748292","Meta",Address{"California","Texas","US","41010"}},
		{"Zuck", "35","229928292","Microsoft",Address{"Delhi","New Delhi","India","41010"}},
		{"Bill Gates", "15","233748292","qQuick",Address{"Jarkhand","India","","41010"}},
	}
	for _, value := range employees {
		db.Write("users",value.Name, User {
			Name: value.Name,
			Age: value.Age,
			Contact: value.Contact,
			Company: value.Company,
			Address: value.Address,
		})
	}

	records, err := db.ReadAll("users")
	if err != nil {
		fmt.Println("Error",err) 
	}
	fmt.Println(records)

	allusers := []User{}
	for _ , value := range records{
		employeeFound := User{}
		if err := json.Unmarshal([]byte(value), &employeeFound); err != nil{
			fmt.Println("Error",err)
		}
		allusers = append(allusers, employeeFound)
	}
	fmt.Println((allusers))

	// if err := db.Delete("users","Ronaldo"); err != nil {
	// 	fmt.Println("Error",err)
	// }

	// if err := db.Delete("users",""); err!= nil {
	// 	fmt.Println("Error",err)
	// }
}

//struct methods