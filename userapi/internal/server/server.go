package server

import (
	"context"
	"errors"
	"fmt"
	"github.com/gin-gonic/gin"
	"log"
	"net/http"
	"userapi/internal/domain"
	"userapi/internal/kafka"
	"userapi/internal/postgresql"
)

type Server struct {
	port string
	pg   *postgresql.PgClient
}

type Response struct {
	Status  string `json:"status"`
	Message string `json:"message"`
}

func New(port string, client *postgresql.PgClient) *Server {
	return &Server{
		port: port,
		pg:   client,
	}
}

func (s *Server) newApi() *gin.Engine {
	g := gin.New()
	g.POST("/scenario", s.manageScenarioHandler)
	g.GET("/scenario/:id", s.getStatusHandler)
	g.GET("/prediction/:id", s.getPredictionHandler)
	return g
}

func (s *Server) getPredictionHandler(ctx *gin.Context) {
	id := ctx.Param("id")

	predictions, err := s.pg.GetPredictions(ctx, id)

	if err != nil {
		log.Printf("[userapi] Error getting predictions: %v", err)
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	ctx.JSON(http.StatusOK, gin.H{"status": "ok", "predictions": predictions})
}

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

func (s *Server) getStatusHandler(ctx *gin.Context) {
	id := ctx.Param("id")

	status, err := s.pg.GetScenarioStatus(context.Background(), id)
	if err != nil {
		if errors.Is(err, postgresql.ErrNoScenario) {
			ctx.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
			return
		}
		log.Printf("[userapi] Error getting status: %v", err)
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	ctx.JSON(http.StatusOK, gin.H{"status": status})
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
