package server

import (
	"errors"
	"fmt"
	"github.com/gin-gonic/gin"
	"log"
	"net/http"
	"orchestrator/internal/domain"
	"orchestrator/internal/postgresql"
	"orchestrator/internal/runners"
)

type Server struct {
	port       string
	service    *postgresql.PgClient
	runnerPool *runners.ScenarioPool
}

type Response struct {
	Status  string `json:"status"`
	Message string `json:"message"`
}

func New(port string, client *postgresql.PgClient) *Server {
	return &Server{
		port:    port,
		service: client,
	}
}

func (s *Server) newApi() *gin.Engine {
	g := gin.New()
	g.POST("/ping", s.pingRunnerHandler)
	return g
}

func (s *Server) pingRunnerHandler(ctx *gin.Context) {
	var req domain.RunnerHb
	if err := ctx.ShouldBindJSON(&req); err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"code": 400, "error": "bad request"})
		return
	}

	if err := s.runnerPool.AcceptHeartbeat(req.Id); err != nil {
		log.Printf("Failed to accept heartbeat: %v", err)
		if errors.Is(err, runners.ScenarioNotFoundErr) {
			ctx.JSON(http.StatusBadRequest, gin.H{"code": 400, "error": "bad request"})
		} else {
			ctx.JSON(http.StatusInternalServerError, gin.H{"code": 500, "error": "internal server error"})
		}
	}

	ctx.JSON(http.StatusOK, gin.H{"code": 200, "message": "ok"})
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
