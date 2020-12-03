// Copyright 2018 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package api

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"strconv"
	"time"

	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/logutil"
	"github.com/tikv/pd/server"
	"github.com/unrolled/render"
)

type logHandler struct {
	svr *server.Server
	rd  *render.Render
}

func newLogHandler(svr *server.Server, rd *render.Render) *logHandler {
	return &logHandler{
		svr: svr,
		rd:  rd,
	}
}

// @Tags admin
// @Summary Set log level.
// @Accept json
// @Param level body string true "json params"
// @Produce json
// @Success 200 {string} string "The log level is updated."
// @Failure 400 {string} string "The input is invalid."
// @Failure 500 {string} string "PD server failed to proceed the request."
// @Failure 503 {string} string "PD server has no leader."
// @Router /admin/log [post]
func (h *logHandler) SetGlobalLevel(w http.ResponseWriter, r *http.Request) {
	var level string
	data, err := ioutil.ReadAll(r.Body)
	r.Body.Close()
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	err = json.Unmarshal(data, &level)
	if err != nil {
		h.rd.JSON(w, http.StatusBadRequest, err.Error())
		return
	}

	err = h.svr.SetLogLevel(level)
	if err != nil {
		h.rd.JSON(w, http.StatusBadRequest, err.Error())
		return
	}
	log.SetLevel(logutil.StringToZapLogLevel(level))

	h.rd.JSON(w, http.StatusOK, "The log level is updated.")
}

const defaultGetLogSecond = 600 // 10 minutes

// @Tags admin
// @Summary Get logs.
// @Param name query []string true "name"
// @Param second query integer false "duration of getting" collectionFormat(multi)
// @Produce text
// @Success 200 {string} string "Finished getting logs."
// @Failure 400 {string} string "The input is invalid."
// @Router /admin/log [get]
func (h *logHandler) GetLog(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	var second int64
	if secondStr := query.Get("second"); secondStr != "" {
		var err error
		second, err = strconv.ParseInt(secondStr, 10, 64)
		if err != nil {
			h.rd.JSON(w, http.StatusBadRequest, err.Error())
			return
		}
	}
	if second <= 0 {
		second = defaultGetLogSecond
	}

	names := query["name"]
	if len(names) == 0 {
		h.rd.JSON(w, http.StatusBadRequest, "empty name.")
		return
	}

	logConfig := h.svr.GetConfig().Log
	httpLogger, err := logutil.NewHTTPLogger(&logConfig, w)
	if err != nil {
		h.rd.JSON(w, http.StatusBadRequest, err.Error())
		return
	}

	httpLogger.Plug(names...)
	defer httpLogger.Close()

	select {
	case <-time.After(time.Duration(second) * time.Second):
	case <-r.Context().Done():
	}
}
