package main

import "sync"

// TempUserData stores temporary data for a user that is not persisted in the database.
type TempUserData struct {
	WatchedVideos map[string]bool
	TotalLosses   int
	RevengePlays  int
}

var tempUserData = make(map[string]*TempUserData)
var tempUserDataMu sync.Mutex

func GetTempUserData(username string) *TempUserData {
	tempUserDataMu.Lock()
	defer tempUserDataMu.Unlock()

	if _, ok := tempUserData[username]; !ok {
		tempUserData[username] = &TempUserData{
			WatchedVideos: make(map[string]bool),
		}
	}
	return tempUserData[username]
}
