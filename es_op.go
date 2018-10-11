package elk

/*elasticsearch operation for version 6.4.1
  add in 2018.10
*/

import (
	"auas/cmdb"
	"auas/device/query"
	"context"
	"encoding/json"
	"fmt"
	"github.com/gracehttp"
	es "github.com/olivere/elastic"
	"regexp"
	"strings"
	"sync"
	"time"
)

var (
	PREFIX            = "***_"
	DEFAULT_INDEX     = GetDefaultIndex()
	TYPE              = "****"
	GlbCtx, GlbCancel = context.WithCancel(gracehttp.GLOBAL_CTX)
)

var (
	GlobalClient        *es.Client
	GlobalBulkProcessor *es.BulkProcessor
	ALLIP               = regexp.MustCompile(`^(\d{1,2}|1\d\d|2[0-4]\d|25[0-5])\.(\d{1,2}|1\d\d|2[0-4]\d|25[0-5])\.(\d{1,2}|1\d\d|2[0-4]\d|25[0-5])\.(\d{1,2}|1\d\d|2[0-4]\d|25[0-5])`)
	ALLID               = regexp.MustCompile(`^(TYS|TYZ|TYL|TY)`)
	SUBS                = regexp.MustCompile(`([;|,\n\t]|\s{2,})`)
)

type FastDecoder struct { //自定义序列解码器
}

func GetDefaultIndex() string {
	return PREFIX + time.Now().Format("2006-01-02")
}

func (u *FastDecoder) Decode(data []byte, v interface{}) (err error) { //具体解码方式
	return json.Unmarshal(data, v)
}

func beforeCallback(executionId int64, requests []es.BulkableRequest) { //bulk request commit before

}

func afterCallback(executionId int64, requests []es.BulkableRequest, response *es.BulkResponse, err error) { //bulk request commit after

}

func SetupEsClient() (client *es.Client, bp *es.BulkProcessor, err error) { //创建es客户端
	var decodeOpt es.ClientOptionFunc
	if elkConfig.FastDecode {
		decodeOpt = es.SetDecoder(&es.DefaultDecoder{})
	} else {
		decodeOpt = es.SetDecoder(&FastDecoder{})
	}
	sniffOpt := es.SetSniff(!elkConfig.DisableSniff)
	client, err = es.NewClient(es.SetURL(elkConfig.Url), decodeOpt, sniffOpt)
	if err != nil {
		logElk.Error("create new elastic client err: ", err)
		return nil, nil, err
	}
	bp, err = client.BulkProcessor().Name(elkConfig.BulkName).Before(beforeCallback).After(afterCallback).BulkActions(1).FlushInterval(30 * time.Second).Workers(elkConfig.BulkWorkerNumber).Stats(true).Do(GlbCtx)
	return
}

func StringQuery(cond string, isAll bool) (strQuery *es.QueryStringQuery) { //query string
	strQuery = es.NewQueryStringQuery(cond)
	var defaultOpt string = "and"
	if isAll {
		defaultOpt = "or"
	}
	strQuery = strQuery.DefaultOperator(defaultOpt).AnalyzeWildcard(true) //.DefaultField("_all")
	return
}

func CheckIsAllIpOrAssetId(condition string) (cond string, isall bool) { //check if is all ip or asset id
	isall = true
	if len(condition) == 0 {
		return
	}
	cond = SUBS.ReplaceAllString(condition, " ")
	strs := strings.Split(cond, " ")
	tmp := make([]string, 0)
	for _, str := range strs {
		if len(str) == 0 {
			continue
		}
		if !ALLIP.MatchString(str) && !ALLID.MatchString(str) { //have not ip and asset id matched
			isall = false
		}
		t := "\"" + str + "\""
		tmp = append(tmp, t)
	}
	cond = strings.Join(tmp, " ") //全文检索匹配
	return
}

func ReadEs(condition, date string, page, size int, sortBy string) (total int64, devices []*query.DeviceInfo, err error) { //search condition
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
	cond, isall := CheckIsAllIpOrAssetId(condition)
	strquery := StringQuery(cond, isall)

	var ss *es.SearchSource
	if len(sortBy) != 0 { //根据指定字段排序
		asc := true //默认升序
		ss = es.NewSearchSource().Query(strquery).Sort(sortBy, asc).From(from).Size(size)
	} else {
		ss = es.NewSearchSource().Query(strquery).From(from).Size(size)
	}
	ssource, _ := ss.Source()
	logElk.Infof("the search source is: %v", ssource) //调试用

	res, err := GlobalClient.Search(DEFAULT_INDEX).Type(TYPE).SearchSource(ss).Do(GlbCtx)
	if err != nil {
		logElk.Errorf("search by condition err: %v|%v", condition, err)
		return
	}
	if res == nil || res.Hits.TotalHits == 0 {
		logElk.Error("search results is nil.")
		return
	}
	devices = make([]*query.DeviceInfo, 0)
	total = res.Hits.TotalHits
	for _, item := range res.Hits.Hits {
		device := new(query.DeviceInfo)
		if err = json.Unmarshal(*item.Source, device); err != nil {
			logElk.Errorf("decode and deserializes search result err: %v|%v", *item.Source, err)
			continue
		}
		devices = append(devices, device)
	}
	return
}

func DeleteEs(assetId string) (err error) { //delete es by asset id
	if len(assetId) == 0 {
		return
	}
	DEFAULT_INDEX = GetDefaultIndex()
	deleteService := es.NewDeleteService(GlobalClient)
	//logElk.Infof("the delete validate is: %v", deleteService.Validate()) //调试用
	res, err := deleteService.Index(DEFAULT_INDEX).Type(TYPE).Id(assetId).Do(GlbCtx)
	if err != nil {
		logElk.Errorf("delete device err: %v", err)
		return
	}
	if res.Status == 404 {
		err = fmt.Errorf("device[%v] not found", assetId)
	}
	logElk.Infof("the delete response is: %v|%v", res.Status, res.Result) //调试用
	return
}

func BulkWrite(devices []*query.DeviceInfo, client *es.Client, index string) (err error) { //执行批量写入
	if len(devices) == 0 {
		return nil
	}
	//logElk.Infof("***** %v ***** %v *****", len(devices), index)
	bulkService := client.Bulk()
	for _, device := range devices {
		Id := fmt.Sprintf("%v", device.AssetId)
		req := es.NewBulkIndexRequest().Index(index).Type(TYPE).Id(Id).Doc(device)
		bulkService.Add(req)
		//GlobalBulkProcessor.Add(req)
	}
	/*BulkResponse*/ _, err = bulkService.Do(GlbCtx)
	if err != nil {
		logElk.Errorf("bulk index err: %v", err)
		return
	}
	return
}

func WriteEs(devices []*query.DeviceInfo) (err error) {
	DEFAULT_INDEX = GetDefaultIndex()
	cnt := len(devices)
	if cnt == 0 {
		return nil
	}
	for start := 0; start < cnt; {
		end := start + 1000
		if end > cnt {
			end = cnt
		}
		err = BulkWrite(devices[start:end], GlobalClient, DEFAULT_INDEX)
		if err != nil {
			logElk.Errorf("bulk write err: %v|index: %v", err, DEFAULT_INDEX)
		}
		start = end
	}
	return
}

func GetAllDevices(indexName string) (err error) { //获取所有设备信息
	s := `select *`
	asset_ids := make([]string, 0)
	rows, err := tuskDB.Query(s)
	if err != nil {
		logElk.Errorf("query all asset id err: %v", err)
		return
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		err = rows.Scan(&id)
		if err != nil {
			logElk.Errorf("scan asset id err: %v", err)
			continue
		}
		asset_ids = append(asset_ids, id)
	}
	cnt := len(asset_ids)
	logElk.Info("the total device of CFD: ", cnt)
	var wg sync.WaitGroup
	for index := 0; index < cnt; {
		end := index + 3000
		if end > cnt {
			end = cnt
		}
		idstr := strings.Join(asset_ids[index:end], ";")
		index = end
		r, err := cmdb.GetDeviceInfo(idstr, "")
		if err != nil {
			logElk.Errorf("batch query device info from cmdb by asset ids err: %v", err)
			continue
		}
		if len(r) == 0 {
			logElk.Errorf("batch query by asset ids from cmdb no results returned")
			continue
		}
		devices := make([]*query.DeviceInfo, 0)
		for _, info := range r {
			if info == nil {
				continue
			}
			dev := GetLocalDeviceInfoFromCmdb(info)
			if dev == nil {
				logElk.Errorf("get local device info from cmdb err: %v", info.SvrAssetId)
				continue
			}
			devices = append(devices, dev)
		}
		scnt := len(devices)
		wg.Add(1)
		go func(ct int, dev []*query.DeviceInfo) {
			defer wg.Done()
			/*for start := 0; start < ct; {
				to := start + 1000
				if to > ct {
					to = ct
				}
				err = BulkWrite(dev[start:to], GlobalClient, indexName)
				if err != nil {
					logElk.Errorf("bulk write err: %v|index: %v", err, indexName)
				}
				start = to
			}*/
			WriteEs(dev)
		}(scnt, devices)
	}
	wg.Wait()
	return
}

func SyncDeviceInfo(index string) (err error) { //全量同步, 从db or cmdb同步?
	err = GetAllDevices(index)
	if err != nil {
		logElk.Errorf("get all device info when sync err: %v", err)
	}
	return
}

func IndexTimer() { //定时创建索引和索引数据
	go func(ctx context.Context) {
		for {
			select {
			case <-ctx.Done():
				logElk.Info("auas server have shut down, exit index.")
				GlobalBulkProcessor.Close()
				GlobalClient.Stop()
				GlbCancel()
				return
			default:
				client := GlobalClient
				now := time.Now()
				tomorrow := now.AddDate(0, 0, 1).Format("2006-01-02")
				//tomorrow := now.AddDate(0, 0, 0).Format("2006-01-02") //测试用
				toindex := time.Date(now.Year(), now.Month(), now.Day(), 22, 0, 0, 0, now.Location())
				//toindex := time.Date(now.Year(), now.Month(), now.Day(), 11, 15, 0, 0, now.Location()) //测试用
				t := time.NewTimer(toindex.Sub(now)) //每天晚上22:00创建新索引
				<-t.C
				exists, err := client.IndexExists(PREFIX + tomorrow).Do(ctx)
				if err != nil {
					logElk.Error(err)
				}
				if exists {
					_, err := client.DeleteIndex(PREFIX + tomorrow).Do(ctx)
					if err != nil {
						logElk.Errorf("delete exits index err: %v|%v", PREFIX+tomorrow, err)
					}
				}
				createIndex, err := client.CreateIndex(PREFIX + tomorrow).Do(ctx)
				if err != nil || !createIndex.Acknowledged {
					logElk.Errorf("create new index for tomorrow failed: %v|%v", err, createIndex.Acknowledged)
					continue
				}
			}
		}
	}(GlbCtx)
	go func(ctx context.Context) {
		for {
			select {
			case <-ctx.Done():
				logElk.Info("auas server have shut down, exit sync.")
				GlobalBulkProcessor.Close()
				GlobalClient.Stop()
				GlbCancel()
				return
			default:
				now := time.Now()
				tomorrow := now.AddDate(0, 0, 1).Format("2006-01-02")
				//tomorrow := now.AddDate(0, 0, 0).Format("2006-01-02") //测试用
				next := now.Add(time.Hour * 24) //取第二天当前时间
				//next := now //测试用
				tosync := time.Date(next.Year(), next.Month(), next.Day(), 06, 0, 0, 0, next.Location())
				//tosync := time.Date(next.Year(), next.Month(), next.Day(), 12, 50, 0, 0, next.Location()) //测试用
				ts := time.NewTimer(tosync.Sub(now)) //第二天凌晨6点执行全量同步
				<-ts.C
				err := SyncDeviceInfo(PREFIX + tomorrow) //索引成功后定时进行设备信息同步
				if err != nil {
					logElk.Error("sync all device info from db to es failed: ", err)
				}
			}
		}
	}(GlbCtx)
}

func Init() {
	var err error
	GlobalClient, GlobalBulkProcessor, err = SetupEsClient()
	if err != nil {
		logElk.Error("create es client err: ", err)
	}
	if !elkConfig.DisableSync { //控制后台server的同步
		IndexTimer()
	}
}
