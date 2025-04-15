package server

import (
	"awesomeProject/internal/kafka"
	"encoding/json"
	"fmt"
	"github.com/gin-gonic/gin"
	"net/http"
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
	g.POST("/notify", s.sendNotificationHandler)
	return g
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
