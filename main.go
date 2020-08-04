package main

import "time"

func main() {
	const EPOCH = int64(1577808000000)
	var curTime = time.Now()
	var epoch = curTime.Add(time.Unix(EPOCH/1000, (EPOCH%1000)*1000000).Sub(curTime))
	now := time.Since(epoch).Nanoseconds() / 1000000
	println("now", now)
}
