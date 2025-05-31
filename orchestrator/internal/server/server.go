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

func New(port string, client *postgresql.PgClient, rp *runners.ScenarioPool) *Server {
	return &Server{
		port:       port,
		service:    client,
		runnerPool: rp,
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

	// TODO: Либо убрать runnerPool, либо создавать каждый раз этот раннер, потому что он не находит его, чтобы принять хартбит
	// NOTE: Может быть уже не проблема
	if s.runnerPool == nil {
		log.Println("[orchestrator] runnerPool is nil")
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": "runnerPool is nil"})
		return
	}

	if err := s.runnerPool.AcceptHeartbeat(req.Id); err != nil {
		log.Printf("[orchestrator] Failed to accept heartbeat: %v", err)
		if errors.Is(err, runners.ScenarioNotFoundErr) {
			ctx.JSON(http.StatusBadRequest, gin.H{"code": 400, "error": "scenario not found"})
		} else if errors.Is(err, runners.ScenarioNotActiveErr) {
			ctx.JSON(http.StatusBadRequest, gin.H{"code": 400, "error": "scenario not active"})
		} else {
			ctx.JSON(http.StatusInternalServerError, gin.H{"code": 500, "error": "internal server error"})
		}
	}

	ctx.JSON(http.StatusOK, gin.H{"code": 200, "message": "ok"})
}

func (s *Server) Run() error {
	eng := s.newApi()
	log.Printf("[orchestrator] Client is up and running with client.runnerPool: %v", s.runnerPool == nil)
	err := eng.Run(":" + s.port)
	if err != nil {
		fmt.Println("[orchestrator] server run err:", err)
		return err
	}
	return nil
}
