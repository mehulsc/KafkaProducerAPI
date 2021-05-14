package api

import (
	"net/http"
	"strings"
	"time"

	"log"

	"github.com/gin-gonic/gin"
)

// ContentTypeJSONRequired enforces Content-Type: application/json in a POST/PATCH request.
func ContentTypeJSONRequired() gin.HandlerFunc {
	return func(c *gin.Context) {
		if c.Request.Method == http.MethodPost || c.Request.Method == http.MethodPatch {
			contentType := c.GetHeader("Content-Type")
			if !isContentTypeJSON(contentType) {
				c.JSON(http.StatusUnsupportedMediaType, NewHTTPErrorResponse(http.StatusUnsupportedMediaType, ""))
				c.Abort()
				return
			}

			c.Next()
		}
	}
}

// isContentTypeJSON returns true if the content type is application/json,
// allowing the content-type string to include extra parameters like charset=utf-8,
// for example `Content-Type: application/json; charset=utf-8` will return true.
func isContentTypeJSON(contentType string) bool {
	return contentType == ContentTypeJSON || strings.HasPrefix(contentType, ContentTypeJSON+";")
}

// ElapsedHandler records and logs an HTTP request with the elapsed time and status code
func ElapsedHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		// start time
		startTime := time.Now()
		// Processing request
		c.Next()
		// End time
		endTime := time.Now()
		// execution time
		latencyTime := endTime.Sub(startTime)
		// Request mode
		reqMethod := c.Request.Method
		// Request routing
		reqURI := c.Request.RequestURI
		// Status code
		statusCode := c.Writer.Status()
		// Request IP
		clientIP := c.ClientIP()
		// Log format
		log.Printf("| %3d | %13v | %15s | %s | %s ",
			statusCode,
			latencyTime,
			clientIP,
			reqMethod,
			reqURI,
		)
	}
}
