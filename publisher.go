//  Copyright 2017 Jeff Nickoloff "jeff@allingeek.com"
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
package events

import (
	"log"
	uurl "net/url"
	"time"

	influx "github.com/influxdata/influxdb/client"
)

func PublishLog() chan EventBlock {
	c := make(chan EventBlock, 1)
	log.Println(`publisher started`)
	go func() {
		for {
			select {
			case eb := <-c:
				log.Printf("publishing event: %v\n", eb)
			}
		}
	}()
	return c
}

func PublishInflux(url, db string, maxIdle time.Duration) chan EventBlock {
	c := make(chan EventBlock, 100)

	u, err := uurl.Parse(url)
	if err != nil {
		log.Printf("unable to parse InfluxDB url %s. err=%v", url, err)
		close(c)
		return c
	}

	log.Println(`influx publisher started`)
	go func(url *uurl.URL, db string, maxIdle time.Duration) {
		client, err := influx.NewClient(influx.Config{URL: *url})
		if err != nil {
			log.Println(`unable to create IncludDB client: ` + err.Error())
			close(c)
			return
		}
		for {
			select {
			case <-time.After(maxIdle):
				_, _, err := client.Ping()
				if err != nil {
					log.Println(`ping failed - unsubscribing`)
					close(c)
					return
				}
			case eb := <-c:
				var pts []influx.Point
				now := time.Now()
				for _, e := range eb.events {
					pts = append(pts, influx.Point{Time: now, Measurement: e.Name(), Tags: eb.tags, Fields: map[string]interface{}{`value`: e.Value()}})
				}
				_, err := client.Write(influx.BatchPoints{Points: pts, Database: db})
				if err != nil {
					log.Println(`event publication failed: ` + err.Error())
					close(c)
					return
				}
			}
		}
	}(u, db, maxIdle)
	return c
}
