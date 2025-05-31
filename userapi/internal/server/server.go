package server

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"net/http"
	"userapi/internal/domain"
	"userapi/internal/kafka"
)

type Server struct {
	port string
}

type Response struct {
	Status  string `json:"status"`
	Message string `json:"message"`
}

func New(port string) *Server {
	return &Server{
		port: port,
	}
}

func (s *Server) newApi() *gin.Engine {
	g := gin.New()
	g.POST("/scenario", s.manageScenarioHandler)
	//TODO: Finish when everything else works
	//g.GET("/scenario/:id", s.getScenarioHandler)
	//g.GET("/prediction/:id", s.getPredictionHandler)
	return g
}

// TODO: Добавить проверку на текущий статус, чтобы не отправлять лишние уведомления и давать какой-то осмысленный ответ
func (s *Server) manageScenarioHandler(ctx *gin.Context) {

	var req domain.RunnerMsg
	if err := ctx.ShouldBind(&req); err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	kafka.ChangeRunnerState(req)

	response := Response{
		Status:  "ok",
		Message: fmt.Sprintf("Notification with Id(%s) and Action(%d) was successfully sent", req.Id, req.Action),
	}

	ctx.Header("Content-Type", "application/json")

	ctx.JSON(http.StatusOK, response)
}

func (s *Server) Run() error {
	eng := s.newApi()
	err := eng.Run(":" + s.port)
	if err != nil {
		fmt.Println("[userapi] server run err:", err)
		return err
	}
	return nil
}
