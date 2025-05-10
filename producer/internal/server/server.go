package server

import (
	"encoding/json"
	"fmt"
	"github.com/gin-gonic/gin"
	"io"
	"log"
	"net/http"
	"producer/internal/kafka"
	"producer/internal/runner"
)

type Server struct {
	port   string
	runner *runner.Runner
}

type Response struct {
	Status  string `json:"status"`
	Message string `json:"message"`
}

type StartRunnerRequest struct {
	Id string `json:"id"`
}

func New(port string) *Server {
	return &Server{
		port:   port,
		runner: runner.NewRunner(),
	}
}

func (s *Server) newApi() *gin.Engine {
	g := gin.New()
	g.POST("/notify", s.sendNotificationHandler)
	g.POST("/start", s.startRunnerHandler)
	g.POST("/stop", s.stopRunnerHandler)
	return g
}

func (s *Server) startRunnerHandler(ctx *gin.Context) {
	var req StartRunnerRequest
	if err := ctx.ShouldBindJSON(&req); err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	var runnerStatus string

	if !s.runner.Start(req.Id) {
		runnerStatus = "is already running"
	} else {
		runnerStatus = "started successfully"
	}

	response := Response{
		Status:  "ok",
		Message: "Runner with job " + runnerStatus,
	}

	ctx.Header("Content-Type", "application/json")

	ctx.JSON(http.StatusOK, response)
}

func (s *Server) stopRunnerHandler(ctx *gin.Context) {
	if ctx.Request.Body != http.NoBody {
		_, _ = io.Copy(io.Discard, ctx.Request.Body)
		defer func(Body io.ReadCloser) {
			err := Body.Close()
			if err != nil {
				log.Printf("Failed to close body: %v", err)
			}
		}(ctx.Request.Body)
	}

	var runnerStatus string

	if !s.runner.Stop() {
		runnerStatus = "is already stopped"
	} else {
		runnerStatus = "stopped successfully"
	}

	response := Response{
		Status:  "ok",
		Message: "Runner with job " + runnerStatus,
	}

	ctx.Header("Content-Type", "application/json")

	ctx.JSON(http.StatusOK, response)
}

func (s *Server) sendNotificationHandler(ctx *gin.Context) {

	var req kafka.Notification
	if err := ctx.ShouldBind(&req); err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	reqBytes, err := json.Marshal(req)
	if err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	kafka.PushNotificationToQueue("notifications", reqBytes)

	response := Response{
		Status:  "ok",
		Message: fmt.Sprintf("Notification with title(%s) and timestamp(%d) was successfully sent", req.Title, req.Timestamp),
	}

	ctx.Header("Content-Type", "application/json")

	ctx.JSON(http.StatusOK, response)
}

func (s *Server) Run() error {
	eng := s.newApi()
	err := eng.Run(":" + s.port)
	if err != nil {
		fmt.Println("server run err:", err)
		return err
	}
	return nil
}
