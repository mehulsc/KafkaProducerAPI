package api

import (
	"context"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/gin-contrib/gzip"
	"github.com/gin-gonic/gin"

	"github.com/hello-error/KafkaProducerAPI/pkg/producer"
)

const (
	defaultReadTimeout    = time.Second * 10
	defaultWriteTimeout   = time.Second * 60
	defaultIdleTimeout    = time.Second * 120
	serverShutdownTimeout = time.Second * 5

	// ContentTypeJSON json content type header
	ContentTypeJSON = "application/json"
)

// API exposes an HTTP API
type API struct {
	server *http.Server
	kafka  *producer.KafkaService
}

// HTTPResponse represents the http response struct
type HTTPResponse struct {
	Error *HTTPError  `json:"error,omitempty"`
	Data  interface{} `json:"data,omitempty"`
}

// HTTPError is included in an HTTPResponse
type HTTPError struct {
	Message string `json:"message"`
	Code    int    `json:"code"`
}

// NewHTTPErrorResponse returns an HTTPResponse with the Error field populated
func NewHTTPErrorResponse(code int, msg string) HTTPResponse {
	if msg == "" {
		msg = http.StatusText(code)
	}

	return HTTPResponse{
		Error: &HTTPError{
			Code:    code,
			Message: msg,
		},
	}
}

// Create creates a new Server instance that listens on HTTP
func Create(host string, kafka *producer.KafkaService) *API {
	api := &API{kafka: kafka}
	router := api.newRouter()

	api.server = &http.Server{
		Addr:         host,
		Handler:      router,
		ReadTimeout:  defaultReadTimeout,
		WriteTimeout: defaultWriteTimeout,
		IdleTimeout:  defaultIdleTimeout,
	}

	return api
}

// Run starts the Service
func (a *API) Run() error {
	var wg sync.WaitGroup

	errs := make(chan error, 1)

	// start kafka produce handling
	go a.kafka.DeliveryReports()

	// start the api server
	wg.Add(1)
	go func() {
		defer wg.Done()

		errs <- a.Serve()
	}()

	// wait for ctrl+c and shutdown the server gracefully
	quit := make(chan struct{})
	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, syscall.SIGINT)
		<-c
		close(quit)
	}()

	var err error
	select {
	case <-quit:
	case err = <-errs:
		log.Printf("goroutine failed: %v", err.Error())
	}

	// Shutdown the HTTP server with a timeout
	// Note: the HTTP server may not be running if it failed to start
	ctx, cancel := context.WithTimeout(context.Background(), serverShutdownTimeout)
	defer cancel()
	if err := a.server.Shutdown(ctx); err != nil {
		log.Printf("shutdownServer error: %v", err.Error())
	}

	log.Print("waiting for goroutines to finish")
	wg.Wait()

	return err
}

// Serve serves the http rest api on the configured host
func (a *API) Serve() error {
	if err := a.server.ListenAndServe(); err != nil {
		if err != http.ErrServerClosed {
			return err
		}
	}

	return nil
}

// newRouter creates a new gin router and initializes all the endpoints & middleware
func (a *API) newRouter() *gin.Engine {
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()

	router.Use(
		gin.Recovery(),
		ContentTypeJSONRequired(),
		ElapsedHandler(),
		gzip.Gzip(gzip.DefaultCompression),
	)
	router.POST("/publish", publishToKafka(a.kafka))

	return router

}

func publishToKafka(kafka *producer.KafkaService) gin.HandlerFunc {
	return func(c *gin.Context) {
		body, err := ioutil.ReadAll(c.Request.Body)
		if err != nil {
			c.JSON(http.StatusBadRequest,
				NewHTTPErrorResponse(http.StatusBadRequest, err.Error()))
			return
		}

		kafka.KProduce(body)
	}
}
