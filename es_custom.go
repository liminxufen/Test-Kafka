package elk

import (
	"auas/device/query"
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
)

var (
	EsReqTpl = `{
		"from": %d,
		"size": %d,
		"query":{	
			"query_string":{
				"default_operator": "%s",	
				"analyze_wildcard": true,
				"query": "%s"
			}
		}
	}`
	EsReqTplOrderBy = `{
		"from": %d,
		"size": %d,
		"sort": "%s",
		"query":{	
			"query_string":{
				"default_operator": "%s",	
				"analyze_wildcard": true,
				"query": "%s"
			}
		}
	}`
)

type QueryStringBody struct {
	DefaultOperator string `json:"default_operator"`
	Query           string `json:"query"`
	AnalyzeWildcard bool   `json:"analyze_wildcard"`
}

type QueryBody struct {
	QueryString QueryStringBody `json:"query_string"`
}

type RawRequestEs struct {
	From  int       `json:"from"`
	Size  int       `json:"size"`
	Sort  string    `json:"sort,omitempty"`
	Query QueryBody `json:"query"`
}

type EsShard struct {
	Total   int `json:"total"`
	Success int `json:"successful"`
	Skipped int `json:"Skipped"`
	Failed  int `json:"failed"`
}

type OuterHits struct {
	Total    int64        `json:"total"`
	MaxScore float64      `json:"max_score"`
	Hits     []*InnerHits `json:"hits"`
}

type InnerHits struct {
	Index  string            `json:"_index"`
	Type   string            `json:"_type"`
	Id     string            `json:"_id"`
	Score  float64           `json:"_score"`
	Source *query.DeviceInfo `json:"_source"`
}

type EsSearchRsp struct {
	Took    float64   `json:"took"`
	TimeOut bool      `json:"time_out"`
	Shard   EsShard   `json:"_shards"`
	Hits    OuterHits `json:"hits"`
}

func CustomRawQuery(condition, date string, page, size int, sortBy string) (total int64, devices []*query.DeviceInfo, err error) { //custom raw query
	if len(condition) == 0 { //无搜索条件则全匹配
		condition = "*"
	}
	if size == 0 {
		size = 10
	}
	if page == 0 {
		page = 1
	}
	if len(date) != 0 {
		DEFAULT_INDEX = PREFIX + date
	} else {
		DEFAULT_INDEX = GetDefaultIndex()
	}
	var from int = (page - 1) * size
	_, cond, isall := CheckIsAllIpOrAssetId(condition)
	defaultOpt := "and"
	if isall {
		defaultOpt = "or"
	}
	var jsonreq string
	if len(sortBy) != 0 {
		jsonreq = fmt.Sprintf(EsReqTplOrderBy, from, size, sortBy, defaultOpt, cond)
	} else {
		jsonreq = fmt.Sprintf(EsReqTpl, from, size, defaultOpt, cond)
	}
	logElk.Infof("es_custom req json format: %s", jsonreq)
	c := &http.Client{}
	//logElk.Infof("request url: %v", fmt.Sprintf("%s/%s/%s/_search", elkConfig.Url, DEFAULT_INDEX, TYPE))
	es_req, err := http.NewRequest("POST", fmt.Sprintf("%s/%s/%s/_search", elkConfig.Url, DEFAULT_INDEX, TYPE), bytes.NewBuffer([]byte(jsonreq)))
	es_req.ContentLength = int64(len([]byte(jsonreq)))
	es_req.Header.Set("Content-Type", "application/json")
	es_rsp, err := c.Do(es_req)
	defer es_rsp.Body.Close()
	body, _ := ioutil.ReadAll(es_rsp.Body)
	rsp := &EsSearchRsp{}
	err = json.Unmarshal(body, rsp)
	if err != nil {
		logElk.Errorf("es_custom json unmarshal err: %v", err)
		return
	}
	logElk.Infof("es_custom search rsp is: %v", rsp)
	if rsp == nil || rsp.Hits.Total == 0 {
		logElk.Error("es_custom search results is nil.")
		return
	}
	devices = make([]*query.DeviceInfo, 0)
	total = rsp.Hits.Total
	for _, item := range rsp.Hits.Hits {
		devices = append(devices, item.Source)
	}
	return
}
