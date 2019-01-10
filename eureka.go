package goutils

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"math/rand"
	"net/http"
	"sort"
	"sync"
	"time"

	log "github.com/thinkphoebe/golog"
)

var GEurekaPath = "/eureka/apps/"
var GRand = rand.New(rand.NewSource(time.Now().UnixNano()))

type EurekaRegisterReq struct {
	Instance *EurekaInstanceInfo `json:"instance"`
}

type EurekaInstanceInfo struct {
	App                           string            `json:"app"`
	InstanceId                    string            `json:"instanceId"`
	HostName                      string            `json:"hostName"`
	IpAddr                        string            `json:"ipAddr"`
	Port                          Port              `json:"port"`
	Status                        string            `json:"status"`
	DataCenterInfo                DataCenterInfo    `json:"dataCenterInfo"`
	VipAddress                    string            `json:"vipAddress,omitempty"`
	SecureVipAddress              string            `json:"secureVipAddress,omitempty"`
	SecurePort                    Port              `json:"securePort,omitempty"`
	HomePageUrl                   string            `json:"homePageUrl,omitempty"`
	StatusPageUrl                 string            `json:"statusPageUrl,omitempty"`
	HealthCheckUrl                string            `json:"healthCheckUrl,omitempty"`
	Metadata                      map[string]string `json:"metadata,omitempty"`
	OverriddenStatus              string            `json:"overriddenStatus,omitempty"`
	CountryId                     int               `json:"countryId,omitempty"`
	LeaseInfo                     LeaseInfo         `json:"leaseInfo,omitempty"`
	IsCoordinatingDiscoveryServer string            `json:"isCoordinatingDiscoveryServer,omitempty"`
	LastUpdatedTimestamp          string            `json:"lastUpdatedTimestamp,omitempty"`
	LastDirtyTimestamp            string            `json:"lastDirtyTimestamp,omitempty"`
	ActionType                    string            `json:"actionType,omitempty"`

	lastUpdateTime int64
}

type LeaseInfo struct {
	RenewalIntervalInSecs int   `json:"renewalIntervalInSecs,omitempty"`
	DurationInSecs        int   `json:"durationInSecs,omitempty"`
	RegistrationTimestamp int64 `json:"registrationTimestamp,omitempty"`
	LastRenewalTimestamp  int64 `json:"lastRenewalTimestamp,omitempty"`
	EvictionTimeStamp     int64 `json:"evictionTimeStamp,omitempty"`
	ServiceUpTimestamp    int64 `json:"serviceUpTimestamp,omitempty"`
}

type Port struct {
	Dollar  int    `json:"$"`
	Enabled string `json:"@enabled"`
}

type DataCenterInfo struct {
	Class string `json:"@class"`
	Name  string `json:"name"`
}

type Application struct {
	Name      string               `json:"name,omitempty"`
	Instances []EurekaInstanceInfo `json:"instance,omitempty"`
}

type EurekaResponse struct {
	Application  Application        `json:"application,omitempty"`
	Applications Applications       `json:"Applicationscations,omitempty"`
	Instance     EurekaInstanceInfo `json:"applications,omitemptyempty"`
}

type Applications struct {
	VersionDelta string        `json:"VersionDeltasions__delta,omitempty"`
	AppsHashCode string        `json:"apps__hashcode,omitempty"`
	Application  []Application `json:"application"`
}

type endPoint struct {
	url      string
	chCancel chan interface{}
}

type EurekaService struct {
	instance  *EurekaInstanceInfo
	endpoints []endPoint
	heartBeat int
}

type EurekaClient struct {
	app             string
	endpoints       []string
	instances       map[string]*EurekaInstanceInfo
	instanceIds     []string
	refreshInterval int
	instanceTimeout int64

	mutex    sync.Mutex
	chCancel chan interface{}
}

func endpointUrl(eurekaUrl, app string) string {
	return eurekaUrl + GEurekaPath + app
}

func (this *EurekaService) Init(eurekaUrls []string, instance *EurekaInstanceInfo, heartBeat int) {
	this.instance = instance
	this.heartBeat = heartBeat
	for _, url := range eurekaUrls {
		ep := endPoint{}
		ep.url = url
		ep.chCancel = make(chan interface{})
		this.endpoints = append(this.endpoints, ep)
	}
}

func (this *EurekaService) runServiceUp(index int) {
	httpclient := &http.Client{
		Timeout: 30 * time.Second,
	}

	for {
		// ================> do register
		epUrl := endpointUrl(this.endpoints[index].url, this.instance.App)
		reqObj := EurekaRegisterReq{}
		reqObj.Instance = this.instance
		regData, err := json.Marshal(reqObj)
		if err != nil {
			log.Fatalf("[Register] json.Marshal got err [%v], [%#v]", err, reqObj)
			return
		}
		log.Debugf("[Register] [%s] [%s]", epUrl, string(regData))

		doRegister := func() bool {
			log.Infof("[Register] [%s] start register...", epUrl)
			req, err := http.NewRequest("POST", epUrl, bytes.NewReader(regData))
			req.Header.Add("Content-Type", "application/json")
			if err != nil {
				log.Errorf("[Register] [%s] http.NewRequest got err [%v]", epUrl, err)
				return false
			}

			resp, err := httpclient.Do(req)
			if err != nil || resp == nil {
				log.Errorf("[Register] [%s] httpclient.Do got err [%v]", epUrl, err)
				return false
			}
			defer resp.Body.Close()

			bytesValue, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				log.Errorf("[Register] [%s] ioutil.ReadAll got err [%v]", epUrl, err)
				return false
			}
			log.Infof("[Register] [%s] status [%v], resp [%s]", epUrl, resp.Status, string(bytesValue))

			if resp.StatusCode == 204 {
				log.Infof("[Register] [%s] register OK", epUrl)
				return true
			}
			return false
		}

		tickerReq := time.NewTicker(30 * time.Second)
		defer tickerReq.Stop()
		for {
			if doRegister() {
				break
			}
			select {
			case <-this.endpoints[index].chCancel:
				log.Errorf("[Register] [%s] got cancel, exit", epUrl)
				return
			case <-tickerReq.C:
				if doRegister() {
					break
				}
			}
		}

		// ================> do heartbeat
		tickerHB := time.NewTicker(time.Duration(this.heartBeat) * time.Second)
		defer tickerHB.Stop()
		for {
			select {
			case <-this.endpoints[index].chCancel:
				log.Errorf("[HeartBeat] [%s] got cancel, exit", epUrl)
				return
			case <-tickerHB.C:
				epUrl := endpointUrl(this.endpoints[index].url, this.instance.App) + "/" +
					this.instance.InstanceId
				req, err := http.NewRequest("PUT", epUrl, nil)
				req.Header.Add("Content-Type", "application/json")
				resp, err := httpclient.Do(req)
				if err != nil || resp == nil {
					log.Errorf("[HeartBeat] [%s] httpclient.Do got err [%v]", epUrl, err)
					continue
				}
				defer resp.Body.Close()

				bytesValue, err := ioutil.ReadAll(resp.Body)
				if err != nil {
					log.Errorf("[HeartBeat] [%s] ioutil.ReadAll got err [%v]", epUrl, err)
					continue
				}
				log.Infof("[HeartBeat] [%s] status [%v], resp [%s]", epUrl, resp.Status, string(bytesValue))

				if resp.StatusCode == 404 {
					log.Infof("[HeartBeat] [%s] server said 404, goto register", epUrl)
					goto HEART_BEAT_END
				}
			}
		}
	HEART_BEAT_END:
	}
}

func (this *EurekaService) Register() {
	for index := range this.endpoints {
		go this.runServiceUp(index)
	}
}

func (this *EurekaService) UnRegister() {
	for _, ep := range this.endpoints {
		ep.chCancel <- true

		client := &http.Client{
			Timeout: 10 * time.Second,
		}
		epUrl := endpointUrl(ep.url, this.instance.App) + "/" + this.instance.InstanceId
		req, err := http.NewRequest("DELETE", epUrl, nil)
		if err != nil {
			log.Errorf("[UnRegister] [%s] http.NewRequest got err [%s]", epUrl, err)
			continue
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := client.Do(req)
		if err != nil || resp == nil {
			log.Errorf("[UnRegister] [%s] client.Do got err [%s]", epUrl, err)
			continue
		}
		defer resp.Body.Close()
		log.Infof("[UnRegister] [%s] status [%s]", epUrl, resp.Status)
	}
}

func (this *EurekaClient) Init(app string, eurekaUrls []string, refresh int) {
	this.app = app
	this.endpoints = eurekaUrls
	this.refreshInterval = refresh
	this.chCancel = make(chan interface{})
	this.instances = make(map[string]*EurekaInstanceInfo)
	this.instanceTimeout = 30 //TODO
}

func (this *EurekaClient) Start() {
	httpclient := &http.Client{
		Timeout: 30 * time.Second,
	}

	readApp := func(endpoint string) *Application {
		epUrl := endpointUrl(endpoint, this.app)
		req, err := http.NewRequest("GET", epUrl, nil)
		req.Header.Add("Accept", "application/json")
		if err != nil {
			log.Errorf("[EurekaClient] [%s] http.NewRequest got err [%v]", epUrl, err)
			return nil
		}

		resp, err := httpclient.Do(req)
		if err != nil || resp == nil {
			log.Errorf("[EurekaClient] [%s] httpclient.Do got err [%v]", epUrl, err)
			return nil
		}
		defer resp.Body.Close()

		bytesValue, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			log.Errorf("[EurekaClient] [%s] ioutil.ReadAll got err [%v]", epUrl, err)
			return nil
		}

		eresp := EurekaResponse{}
		err = json.Unmarshal(bytesValue, &eresp)
		if err != nil {
			log.Errorf("[EurekaClient] [%s] json.Unmarshal got err [%v], [%s]", epUrl, err, string(bytesValue))
			return nil
		}
		//log.Debugf("[EurekaClient] [%s] [%s](%#v)", epUrl, string(bytesValue), eresp)
		return &eresp.Application
	}

	go func() {
		index := 0
		ticker := time.NewTicker(time.Duration(this.refreshInterval) * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				app := readApp(this.endpoints[index])
				index = (index + 1) % len(this.endpoints)
				if app == nil {
					continue
				}

				this.mutex.Lock()
				for _, inst := range app.Instances {
					if _, ok := this.instances[inst.InstanceId]; !ok {
						log.Infof("[EurekaClient] new instance [%s:%s:%d] for service [%s], meta [%#v]",
							inst.InstanceId, inst.IpAddr, inst.Port.Dollar, inst.App, inst.Metadata)
					}
					inst.lastUpdateTime = time.Now().Unix()
					this.instances[inst.InstanceId] = &inst
				}

				this.instanceIds = make([]string, 0)
				for key, inst := range this.instances {
					if time.Now().Unix()-inst.lastUpdateTime > this.instanceTimeout {
						log.Infof("[EurekaClient] remove timeout instance [%s:%s:%d] for service [%s], meta [%#v]",
							inst.InstanceId, inst.IpAddr, inst.Port.Dollar, inst.App, inst.Metadata)
						delete(this.instances, inst.InstanceId)
						continue
					}
					this.instanceIds = append(this.instanceIds, key)
				}
				sort.Strings(this.instanceIds)

				this.mutex.Unlock()
			case <-this.chCancel:
				return
			}
		}
	}()
}

func (this *EurekaClient) Stop() {
	this.chCancel <- true
}

func (this *EurekaClient) GetInstance() *EurekaInstanceInfo {
	this.mutex.Lock()
	defer this.mutex.Unlock()
	if len(this.instanceIds) <= 0 {
		log.Debugf("[GetInstance] no instance [%s]", this.app)
		return nil
	}
	inst := this.instances[this.instanceIds[GRand.Intn(len(this.instanceIds))]]
	log.Debugf("[GetInstance] got [%s][%v]", this.app, inst)
	return inst
}

func (this *EurekaClient) GetInstanceMatch(metaMatch map[string]string) *EurekaInstanceInfo {
	this.mutex.Lock()
	defer this.mutex.Unlock()

	size := len(this.instanceIds)
	pos := GRand.Intn(size)
NEXTINST:
	for i := 0; i < size; i++ {
		inst := this.instances[this.instanceIds[pos]]
		pos = (pos + 1) % size

		if inst.Metadata == nil {
			continue NEXTINST
		}
		for k, v := range metaMatch {
			if meta, ok := inst.Metadata[k]; ok {
				if meta != v {
					continue NEXTINST
				}
			} else {
				continue NEXTINST
			}
		}

		log.Debugf("[GetInstanceMatch] got [%s][%v]", this.app, inst)
		return inst
	}
	log.Debugf("[GetInstanceMatch] no match instance [%s][%#v]", this.app, metaMatch)
	return nil
}
