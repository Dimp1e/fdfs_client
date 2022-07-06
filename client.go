package fdfs_client

import (
	"fmt"
	"net"
	"strings"
	"sync"
)

type Client struct {
	TrackerPools    map[string]*ConnPool
	StoragePools    map[string]*ConnPool
	StoragePoolLock *sync.RWMutex
	Config          *Config
}

func NewClientWithConfig(configName string) (*Client, error) {
	config, err := NewConfig(configName)
	if err != nil {
		return nil, err
	}
	client := &Client{
		Config:          config,
		StoragePoolLock: &sync.RWMutex{},
	}
	client.TrackerPools = make(map[string]*ConnPool)
	client.StoragePools = make(map[string]*ConnPool)

	for _, addr := range config.TrackerAddr {
		trackerPool, err := NewConnPool(addr, config.MaxConns)
		if err != nil {
			return nil, err
		}
		client.TrackerPools[addr] = trackerPool
	}

	return client, nil
}

func (this *Client) Destory() {
	if this == nil {
		return
	}
	for _, pool := range this.TrackerPools {
		pool.Destory()
	}
	for _, pool := range this.StoragePools {
		pool.Destory()
	}
}

func (this *Client) UploadByFilename(fileName string) (string, error) {
	fileInfo, err := newFileInfo(fileName, nil, "")
	defer fileInfo.Close()
	if err != nil {
		return "", err
	}

	storageInfo, err := this.queryStorageInfoWithTracker(TRACKER_PROTO_CMD_SERVICE_QUERY_STORE_WITHOUT_GROUP_ONE, "", "")
	if err != nil {
		return "", err
	}

	task := &storageUploadTask{}
	//req
	task.fileInfo = fileInfo
	task.storagePathIndex = storageInfo.storagePathIndex

	if err := this.doStorage(task, storageInfo); err != nil {
		return "", err
	}
	return task.fileId, nil
}

func (this *Client) UploadByBuffer(buffer []byte, fileExtName string) (string, error) {
	index := strings.LastIndexByte(fileExtName, '.')
	if index != -1 {
		fileExtName = fileExtName[index+1:]
		if len(fileExtName) > 6 {
			fileExtName = fileExtName[:6]
		}
	}
	fileInfo, err := newFileInfo("", buffer, fileExtName)
	defer fileInfo.Close()
	if err != nil {
		return "", err
	}
	storageInfo, err := this.queryStorageInfoWithTracker(TRACKER_PROTO_CMD_SERVICE_QUERY_STORE_WITHOUT_GROUP_ONE, "", "")
	if err != nil {
		return "", err
	}

	task := &storageUploadTask{}
	//req
	task.fileInfo = fileInfo
	task.storagePathIndex = storageInfo.storagePathIndex

	if err := this.doStorage(task, storageInfo); err != nil {
		return "", err
	}
	return task.fileId, nil
}

func (this *Client) DownloadToFile(fileId string, localFilename string, offset int64, downloadBytes int64) error {
	groupName, remoteFilename, err := splitFileId(fileId)
	if err != nil {
		return err
	}
	storageInfo, err := this.queryStorageInfoWithTracker(TRACKER_PROTO_CMD_SERVICE_QUERY_FETCH_ONE, groupName, remoteFilename)
	if err != nil {
		return err
	}

	task := &storageDownloadTask{}
	//req
	task.groupName = groupName
	task.remoteFilename = remoteFilename
	task.offset = offset
	task.downloadBytes = downloadBytes

	//res
	task.localFilename = localFilename

	return this.doStorage(task, storageInfo)
}

//deprecated
func (this *Client) DownloadToBuffer(fileId string, offset int64, downloadBytes int64) ([]byte, error) {
	groupName, remoteFilename, err := splitFileId(fileId)
	if err != nil {
		return nil, err
	}
	storageInfo, err := this.queryStorageInfoWithTracker(TRACKER_PROTO_CMD_SERVICE_QUERY_FETCH_ONE, groupName, remoteFilename)
	if err != nil {
		return nil, err
	}

	task := &storageDownloadTask{}
	//req
	task.groupName = groupName
	task.remoteFilename = remoteFilename
	task.offset = offset
	task.downloadBytes = downloadBytes

	//res
	if err := this.doStorage(task, storageInfo); err != nil {
		return nil, err
	}
	return task.buffer, nil
}

func (this *Client) DownloadToAllocatedBuffer(fileId string, buffer []byte, offset int64, downloadBytes int64) error {
	groupName, remoteFilename, err := splitFileId(fileId)
	if err != nil {
		return err
	}
	storageInfo, err := this.queryStorageInfoWithTracker(TRACKER_PROTO_CMD_SERVICE_QUERY_FETCH_ONE, groupName, remoteFilename)
	if err != nil {
		return err
	}

	task := &storageDownloadTask{}
	//req
	task.groupName = groupName
	task.remoteFilename = remoteFilename
	task.offset = offset
	task.downloadBytes = downloadBytes
	task.buffer = buffer //allocate buffer by user

	//res
	if err := this.doStorage(task, storageInfo); err != nil {
		return err
	}
	return nil
}

func (this *Client) DeleteFile(fileId string) error {
	groupName, remoteFilename, err := splitFileId(fileId)
	if err != nil {
		return err
	}
	storageInfo, err := this.queryStorageInfoWithTracker(TRACKER_PROTO_CMD_SERVICE_QUERY_FETCH_ONE, groupName, remoteFilename)
	if err != nil {
		return err
	}

	task := &storageDeleteTask{}
	//req
	task.groupName = groupName
	task.remoteFilename = remoteFilename

	return this.doStorage(task, storageInfo)
}

func (this *Client) doTracker(task task) error {
	trackerConn, err := this.getTrackerConn()
	if err != nil {
		return err
	}
	defer trackerConn.Close()

	if err := task.SendReq(trackerConn); err != nil {
		return err
	}
	if err := task.RecvRes(trackerConn); err != nil {
		return err
	}

	return nil
}

func (this *Client) doStorage(task task, storageInfo *storageInfo) error {
	storageConn, err := this.getStorageConn(storageInfo)
	if err != nil {
		return err
	}
	defer storageConn.Close()

	if err := task.SendReq(storageConn); err != nil {
		return err
	}
	if err := task.RecvRes(storageConn); err != nil {
		return err
	}

	return nil
}

func (this *Client) queryStorageInfoWithTracker(cmd int8, groupName string, remoteFilename string) (*storageInfo, error) {
	task := &trackerTask{}
	task.cmd = cmd
	task.groupName = groupName
	task.remoteFilename = remoteFilename

	if err := this.doTracker(task); err != nil {
		return nil, err
	}
	return &storageInfo{
		addr:             fmt.Sprintf("%s:%d", task.ipAddr, task.port),
		storagePathIndex: task.storePathIndex,
	}, nil
}

func (this *Client) getTrackerConn() (net.Conn, error) {
	var trackerConn net.Conn
	var err error
	var getOne bool
	for _, trackerPool := range this.TrackerPools {
		trackerConn, err = trackerPool.get()
		if err == nil {
			getOne = true
			break
		}
	}
	if getOne {
		return trackerConn, nil
	}
	if err == nil {
		return nil, fmt.Errorf("no connPool can be use")
	}
	return nil, err
}

func (this *Client) getStorageConn(storageInfo *storageInfo) (net.Conn, error) {
	this.StoragePoolLock.Lock()
	storagePool, ok := this.StoragePools[storageInfo.addr]
	if ok {
		this.StoragePoolLock.Unlock()
		return storagePool.get()
	}
	storagePool, err := NewConnPool(storageInfo.addr, this.Config.MaxConns)
	if err != nil {
		this.StoragePoolLock.Unlock()
		return nil, err
	}
	this.StoragePools[storageInfo.addr] = storagePool
	this.StoragePoolLock.Unlock()
	return storagePool.get()
}
