package main

import (
	"fmt"
	"time"

	redigo "github.com/gomodule/redigo/redis"
)

const addr = "127.0.0.1:6380"

func main() {
	c, err := redigo.DialQ(addr)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	var reply interface{}
	for {
		err = c.Send("ping")
		if err != nil {
			fmt.Println(err.Error())
			return
		}
		err = c.Flush()
		if err != nil {
			fmt.Println(err.Error())
			return
		}
		reply, err = c.Receive()
		if err != nil {
			fmt.Println(err.Error())
			return
		}
		if v, ok := reply.(string); ok {
			fmt.Println(v)
		} else {
			fmt.Println(reply)
		}
		time.Sleep(2 * time.Second)
		for i := 0; i < 10; i++ {
			err = c.Send("abc", "123", "45")
			if err != nil {
				fmt.Println(err.Error())
				return
			}
		}
		err = c.Flush()
		if err != nil {
			fmt.Println(err.Error())
			return
		}
		for i := 0; i < 10; i++ {
			reply, err = c.Receive()
			if err != nil {
				fmt.Println(err.Error())
				return
			}
			fmt.Println(reply)
			l := reply.([]interface{})
			for i := range l {
				item := l[i].([]byte)
				if i == 0 && string(item) != "abc" {
					panic(string(item) + ":abc")
				}
				if i == 1 && string(item) != "123" {
					panic(string(item) + ":123")
				}
				if i == 2 && string(item) != "45" {
					panic(string(item) + ":45")
				}
			}

		}
	}

}
